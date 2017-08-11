#include "rtlsdr.h"
#include <stdio.h>
#include <stdlib.h>
#define _USE_MATH_DEFINES
#include <math.h>
#include <time.h>
#include <string.h>
#include <malloc.h>

#ifdef _WIN32
#include <WinSock2.h>
#include <Ws2tcpip.h>
#include <Windows.h>
#include <src\win32\getopt.h>
#else
#include <netinet/in.h>
#include <netdb.h>
#include <getopt.h>
#endif

// TODO list:
// * Write automated tests!
// * Optimise: move as much as possible to compile-time constants
// * Optimise: investigate using an IIR filter for decimation (performance)
// * Store failed RX messages for investigation (above certain power level)
// * Figure out why some of the messages fail to decode with enough SNR
// * Measure and report SNR
// * Port and test-run on a RPi
// * Figure out the issue with linear regression
// * Collect and output channel statistics

//#define DEBUG_DUMP_CSV_DATA
//#define DEBUG_WRITE_RAW_IF

//
// Some math defines
//

// Define the "real" type to control precision of computations in one place
typedef float real_t;
#define _R(x) ((real_t)(x))

#define R_PI _R(M_PI)
#define R_TWOPI _R(2 * M_PI)
#define R_1_PI _R(M_1_PI)

//
// DSP routines
//

typedef struct _complex_t {
    real_t re;
    real_t im;
} complex_t;

void fir_normalize_f32(unsigned length, real_t *data)
{
    unsigned i;
    real_t k = _R(0);
    for (i = 0; i < length; ++i) {
        k += data[i];
    }
    k = _R(1) / k;
    for (i = 0; i < length; ++i) {
        data[i] *= k;
    }
}

void fir_generate_sinc_f32(real_t fc, unsigned length, real_t *data)
{
    unsigned i;
    real_t w = R_TWOPI * fc;
    int zf = (length - 1) / 2;
    for (i = 0; i < length; ++i) {
        int x = i - zf;
        data[i] = x ? (sinf(w * x) / x) : w;
    }
}

typedef real_t (*window_func_t)(unsigned i, unsigned length);

real_t window_cosine_1(unsigned i, unsigned length, real_t a, real_t b)
{
    real_t w = R_TWOPI / (length - 1);
    return a - b * cosf(w * i);
}

real_t window_cosine_2(unsigned i, unsigned length, real_t a, real_t b, real_t c)
{
    real_t w = R_TWOPI / (length - 1);
    return a - b * cosf(w * i) + c * cosf(2 * w * i);
}

real_t window_hamming(unsigned i, unsigned length)
{
    return window_cosine_1(i, length, 0.54f, 0.46f);
}

real_t window_blackman(unsigned i, unsigned length)
{
    return window_cosine_2(i, length, 0.42f, 0.5f, 0.08f);
}

void fir_apply_window_f32(unsigned length, real_t *data, window_func_t window)
{
    unsigned i;
    for (i = 0; i < length; ++i) {
        data[i] *= window(i, length);
    }
}

real_t fir_convolve_symmetric_rr(const real_t *x, const real_t *h, unsigned length)
{
    unsigned i;
    real_t y = _R(0);

    for (i = 0; i < length; ++i) {
        y += x[i] * h[i];
    }

    return y;
}

complex_t fir_convolve_symmetric_cr(const complex_t *x, const real_t *h, unsigned length)
{
    unsigned i;
    complex_t y;

    y.re = _R(0); y.im = _R(0);
    for (i = 0; i < length; ++i) {
        y.re += x[i].re * h[i];
        y.im += x[i].im * h[i];
    }

    return y;
}

real_t fir_convolve_wrapped(const real_t *x, unsigned offset, const real_t *h, unsigned length)
{
    int i;
    unsigned j;
    real_t y = 0.0f;

    for (i = offset; i >= 0; --i) {
        y += x[i] * *h++;
    }
    for (j = length - 1; j > offset; --j) {
        y += x[j] * *h++;
    }

    return y;
}

//
// Debugging and other general stuff
//

void write_as_csv(const real_t *v, unsigned count, FILE *fp)
{
    while (count--) {
        fprintf(fp, "%+.6f,", *v++);
    }
    fprintf(fp, "\n");
    fflush(fp);
}

#ifdef _WIN32
double get_wall_time(){
    LARGE_INTEGER time,freq;
    if (!QueryPerformanceFrequency(&freq)){
        //  Handle error
        return 0;
    }
    if (!QueryPerformanceCounter(&time)){
        //  Handle error
        return 0;
    }
    return (double)time.QuadPart / freq.QuadPart;
}
double get_cpu_time(){
    FILETIME a,b,c,d;
    if (GetProcessTimes(GetCurrentProcess(),&a,&b,&c,&d) != 0){
        //  Returns total user time.
        //  Can be tweaked to include kernel times as well.
        return
            (double)(d.dwLowDateTime |
            ((unsigned long long)d.dwHighDateTime << 32)) * 0.0000001;
    }else{
        //  Handle error
        return 0;
    }
}

#else

#include <time.h>
#include <sys/time.h>
double get_wall_time(){
    struct timeval time;
    if (gettimeofday(&time,NULL)){
        //  Handle error
        return 0;
    }
    return (double)time.tv_sec + (double)time.tv_usec * .000001;
}
double get_cpu_time(){
    return (double)clock() / CLOCKS_PER_SEC;
}
#endif

//
// ACARS definitions from ARINC SPECIFICATION 618
//

#define ACARS_SYMBOL_RATE 2400

typedef struct _acars_header_t {
    uint8_t soh;
    uint8_t mode;
    char aircraft_reg[7];
    uint8_t tak;
    uint8_t label[2];
    uint8_t block_id;
    uint8_t stx;
} acars_header_t;

#define ISO5_SOH 0x01
#define ISO5_STX 0x02
#define ISO5_ETX 0x03
#define ISO5_NAK 0x15
#define ISO5_SYN 0x16
#define ISO5_ETB 0x17
#define ISO5_DEL 0x7F

//
// Receiver definitions
//

#define RECEIVER_BASEBAND_OVERSAMPLE 16
#define RECEIVER_BASEBAND_SAMPLE_RATE (ACARS_SYMBOL_RATE * RECEIVER_BASEBAND_OVERSAMPLE)
#define RECEIVER_BASEBAND_SAMPLES_PER_BIT (RECEIVER_BASEBAND_OVERSAMPLE)
#define RECEIVER_IF_FILTER_CUTOFF (RECEIVER_BASEBAND_SAMPLE_RATE / 2)
#define RECEIVER_BASEBAND_FILTER_CUTOFF (ACARS_SYMBOL_RATE * 1.414f)
#define RECEIVER_PHASE_ESTIMATE_COUNT 48
#define RECEIVER_PHASE_MEASUREMENTS_MAX 512
#define RECEIVER_DC_MEASUREMENTS_MAX (RECEIVER_BASEBAND_SAMPLES_PER_BIT * 8)
#define RECEIVER_CORRECTABLE_ERRORS_MAX 5
#define RECEIVER_STATION_ID_LENGTH 8
#define RECEIVER_IF_BLOCK_LENGTH (1 << 18)

typedef enum _channel_state_t {
    // Default state where pre-key tone is detected
    CHANNEL_STATE_PREKEY_DETECT,
    // Prekey characteristics are measured, spec. its phase
    CHANNEL_STATE_PREKEY_MEASURE,
    // Syncing to bits and octets
    CHANNEL_STATE_BIT_SYNC,
    // Receiving octets
    CHANNEL_STATE_RECEIVE,
} channel_state_t;

typedef enum _msk_state_t {
    // Previously seen tone was 2400Hz
    MSK_STATE_H0 = 0,
    MSK_STATE_H1 = 1,
    // Previously seen tone was 1200Hz
    MSK_STATE_L0 = 2,
    MSK_STATE_L1 = 3,
} msk_state_t;

typedef enum _acars_state_t {
    ACARS_IDLE,
    ACARS_START,
    ACARS_EXPECT_SYN1,
    ACARS_EXPECT_SYN2,
    ACARS_EXPECT_SOH,
    ACARS_EXPECT_STX,
    ACARS_EXPECT_ETX,
    ACARS_EXPECT_BCS1,
    ACARS_EXPECT_BCS2,
    ACARS_EXPECT_DEL,
} acars_state_t;

typedef struct _channel_t {
    // Channel's center frequency, in Hz
    unsigned fc;
    // The channel's overall state
    channel_state_t state;

    // DDC phasor state
    complex_t ddc_w;
    // DDC multiplier value
    complex_t ddc_dw;
    // DDC decimation filter's input
    complex_t *if_buffer;
    // DDC decimation filter input's x[0] position
    unsigned if_buffer_index;
    // DDC decimation counter
    unsigned if_decim_counter;
    // Baseband AM samples before BB filter
    real_t *baseband_am_unfiltered;
    unsigned baseband_am_unfiltered_index;
    // Baseband AM samples after BB filter
    real_t *baseband_am;
    unsigned baseband_am_index;
    // Prekey detection state
    real_t phase_estimates[RECEIVER_PHASE_ESTIMATE_COUNT];
    real_t phase_estimates_pi[RECEIVER_PHASE_ESTIMATE_COUNT];
    unsigned phase_estimates_index;
    int phase_estimates_ok;
    // DC offset cancellation in baseband
    real_t dc_measurements[RECEIVER_DC_MEASUREMENTS_MAX];
    unsigned dc_measurements_index;
    real_t dc;
    // MSK demod state
    msk_state_t msk_state;
    // Bit sync state
    uint16_t sync_reg;
    unsigned sync_counter;
    // Octet RX state
    uint8_t shift_reg;
    uint8_t bits_in_reg;
    // ACARS message handling
    acars_state_t message_state;
    int ignore_parity;
    unsigned parity_errors;
    uint16_t computed_crc;
    uint16_t received_crc;
    unsigned message_index;
    uint8_t message_buffer[256];
    // Statistics
    unsigned message_prekey_detected;
    unsigned messages_received_successfully;

#ifdef DEBUG_DUMP_CSV_DATA
    FILE *fp_debug_csv;
#endif

} channel_t;

typedef struct _receiver_t {
    // Stuff that doesn't change
    channel_t *channels;
    unsigned channel_count;
    unsigned fc;
    unsigned fs;
    real_t *if_decim_filter;
    unsigned if_decim_filter_length;
    unsigned baseband_decimate_rate;
    real_t *baseband_filter;
    unsigned baseband_filter_length;
    uint8_t station_id[RECEIVER_STATION_ID_LENGTH];
    // Stuff that changes
    complex_t *if_buffer;
    // Output configuration
    uint8_t screen_output_enabled;
    FILE *output_log_fp;
#ifdef _WIN32
    SOCKET output_socket_fd;
#else
    int output_socket_fd;
#endif
    // Debug stuff
    FILE *raw_fp;
} receiver_t;

//
// Message output handlers
//

//
// ACARS message handling
//

#define ACARS_VERBOSE 0

#define CRC_CCITT_START 0

#if 1
static const uint16_t crc_ccitt_table[256] = {
    0x0000, 0x1189, 0x2312, 0x329b, 0x4624, 0x57ad, 0x6536, 0x74bf,
    0x8c48, 0x9dc1, 0xaf5a, 0xbed3, 0xca6c, 0xdbe5, 0xe97e, 0xf8f7,
    0x1081, 0x0108, 0x3393, 0x221a, 0x56a5, 0x472c, 0x75b7, 0x643e,
    0x9cc9, 0x8d40, 0xbfdb, 0xae52, 0xdaed, 0xcb64, 0xf9ff, 0xe876,
    0x2102, 0x308b, 0x0210, 0x1399, 0x6726, 0x76af, 0x4434, 0x55bd,
    0xad4a, 0xbcc3, 0x8e58, 0x9fd1, 0xeb6e, 0xfae7, 0xc87c, 0xd9f5,
    0x3183, 0x200a, 0x1291, 0x0318, 0x77a7, 0x662e, 0x54b5, 0x453c,
    0xbdcb, 0xac42, 0x9ed9, 0x8f50, 0xfbef, 0xea66, 0xd8fd, 0xc974,
    0x4204, 0x538d, 0x6116, 0x709f, 0x0420, 0x15a9, 0x2732, 0x36bb,
    0xce4c, 0xdfc5, 0xed5e, 0xfcd7, 0x8868, 0x99e1, 0xab7a, 0xbaf3,
    0x5285, 0x430c, 0x7197, 0x601e, 0x14a1, 0x0528, 0x37b3, 0x263a,
    0xdecd, 0xcf44, 0xfddf, 0xec56, 0x98e9, 0x8960, 0xbbfb, 0xaa72,
    0x6306, 0x728f, 0x4014, 0x519d, 0x2522, 0x34ab, 0x0630, 0x17b9,
    0xef4e, 0xfec7, 0xcc5c, 0xddd5, 0xa96a, 0xb8e3, 0x8a78, 0x9bf1,
    0x7387, 0x620e, 0x5095, 0x411c, 0x35a3, 0x242a, 0x16b1, 0x0738,
    0xffcf, 0xee46, 0xdcdd, 0xcd54, 0xb9eb, 0xa862, 0x9af9, 0x8b70,
    0x8408, 0x9581, 0xa71a, 0xb693, 0xc22c, 0xd3a5, 0xe13e, 0xf0b7,
    0x0840, 0x19c9, 0x2b52, 0x3adb, 0x4e64, 0x5fed, 0x6d76, 0x7cff,
    0x9489, 0x8500, 0xb79b, 0xa612, 0xd2ad, 0xc324, 0xf1bf, 0xe036,
    0x18c1, 0x0948, 0x3bd3, 0x2a5a, 0x5ee5, 0x4f6c, 0x7df7, 0x6c7e,
    0xa50a, 0xb483, 0x8618, 0x9791, 0xe32e, 0xf2a7, 0xc03c, 0xd1b5,
    0x2942, 0x38cb, 0x0a50, 0x1bd9, 0x6f66, 0x7eef, 0x4c74, 0x5dfd,
    0xb58b, 0xa402, 0x9699, 0x8710, 0xf3af, 0xe226, 0xd0bd, 0xc134,
    0x39c3, 0x284a, 0x1ad1, 0x0b58, 0x7fe7, 0x6e6e, 0x5cf5, 0x4d7c,
    0xc60c, 0xd785, 0xe51e, 0xf497, 0x8028, 0x91a1, 0xa33a, 0xb2b3,
    0x4a44, 0x5bcd, 0x6956, 0x78df, 0x0c60, 0x1de9, 0x2f72, 0x3efb,
    0xd68d, 0xc704, 0xf59f, 0xe416, 0x90a9, 0x8120, 0xb3bb, 0xa232,
    0x5ac5, 0x4b4c, 0x79d7, 0x685e, 0x1ce1, 0x0d68, 0x3ff3, 0x2e7a,
    0xe70e, 0xf687, 0xc41c, 0xd595, 0xa12a, 0xb0a3, 0x8238, 0x93b1,
    0x6b46, 0x7acf, 0x4854, 0x59dd, 0x2d62, 0x3ceb, 0x0e70, 0x1ff9,
    0xf78f, 0xe606, 0xd49d, 0xc514, 0xb1ab, 0xa022, 0x92b9, 0x8330,
    0x7bc7, 0x6a4e, 0x58d5, 0x495c, 0x3de3, 0x2c6a, 0x1ef1, 0x0f78,
};

#define crc_ccitt_update(crc, c) (((crc) >> 8) ^ crc_ccitt_table[((crc) ^ (c)) & 0xFF]);

#else

uint16_t crc_ccitt_update(uint16_t crc, uint8_t data)
{
    data ^= (uint8_t)(crc);
    data ^= data << 4;
    return ((((uint16_t)data << 8) | (crc >> 8)) ^ (uint8_t)(data >> 4) ^ ((uint16_t)data << 3));
}
#endif

static const unsigned char popcount_table[256] = {
    0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,
    1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
    1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
    2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
    1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
    2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
    2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
    3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8
};

int hamming_distance(unsigned a, unsigned b)
{
    int popcount = 0;
    unsigned d = a ^ b;
    while (d) {
        popcount += popcount_table[d & 0xFF];
        d >>= 8;
    }
    return popcount;
}

static void acars_write_to_file(FILE *fp, const char *msg, unsigned msg_length, unsigned fc)
{
    const acars_header_t *header = (acars_header_t *)msg;
    char eom = msg[msg_length - 1];
    time_t tm;
    char timestamp[64];

    tm = time(NULL);
    strftime(timestamp, sizeof(timestamp), "%Y%m%dT%H%M%SZ", gmtime(&tm));
    fprintf(fp, "--[ %d ]-----------------------------------------[ %s ]--\r\n", fc / 1000, timestamp);

    fprintf(fp, "ACARS mode: %c  ", header->mode);
    if (header->aircraft_reg[0]) {
        fprintf(fp, "Aircraft reg: %c%c%c%c%c%c%c\r\n",
            header->aircraft_reg[0], header->aircraft_reg[1], header->aircraft_reg[2],
            header->aircraft_reg[3], header->aircraft_reg[4], header->aircraft_reg[5], header->aircraft_reg[6]);
    } else {
        fprintf(fp, "Aircraft reg: <NUL>\r\n");
    }

    fprintf(fp, "Label: %c%c  ", header->label[0], header->label[1]);
    if (header->block_id) {
        fprintf(fp, "Block ID: %c  ", header->block_id);
    } else {
        fprintf(fp, "Block ID: <NUL>  ");
    }
    fprintf(fp, "More: %c  ", (eom == ISO5_ETX) ? 'N' : 'Y');
    if (header->tak != ISO5_NAK) {
        fprintf(fp, "Tech ACK: %c\r\n", header->tak);
    } else {
        fprintf(fp, "Tech ACK: <NAK>\r\n");
    }
    if (header->stx == ISO5_STX) {
        fprintf(fp, "Message content:\r\n");
        fwrite(&msg[sizeof(acars_header_t)], msg_length - sizeof(acars_header_t) - 1, 1, fp);
    }
    fprintf(fp, "\r\n\r\n");
    fflush(fp);
}

typedef struct _acars_udp_message_header_t acars_udp_message_header_t;
struct _acars_udp_message_header_t {
    uint8_t station_id[RECEIVER_STATION_ID_LENGTH];
    uint32_t timestamp;
    uint32_t fc;
};

typedef struct _acars_udp_message_t acars_udp_message_t;
struct _acars_udp_message_t {
    acars_udp_message_header_t header;
    uint8_t payload[256];
};

static void acars_send_to_fd(receiver_t *recv, channel_t *chan, const char *msg, unsigned msg_length)
{
    acars_udp_message_t pkt;

    memcpy(&pkt.header.station_id[0], &recv->station_id[0], RECEIVER_STATION_ID_LENGTH);
    pkt.header.timestamp = htonl(time(NULL));
    pkt.header.fc = htonl(chan->fc);
    memcpy(&pkt.payload[0], msg, msg_length);
    if (send(recv->output_socket_fd, (char *)&pkt, sizeof(pkt.header) + msg_length, 0) < 0) {
        printf("Failed to send the message over UDP\n");
    }
}

static void acars_process_message(receiver_t *recv, channel_t *chan)
{
    unsigned i;
    unsigned length;

    length = chan->message_index;
    // Clear parity bits as these are no longer needed
    for (i = 0; i < length; ++i) {
        chan->message_buffer[i] &= 0x7F;
    }
    // Fire up all the writers
    if (recv->screen_output_enabled) {
        acars_write_to_file(stdout, (const char *)&chan->message_buffer[0], length, chan->fc);
    }
    if (recv->output_log_fp) {
        acars_write_to_file(recv->output_log_fp, (const char *)&chan->message_buffer[0], length, chan->fc);
    }
    if (recv->output_socket_fd != -1) {
        acars_send_to_fd(recv, chan, (const char *)&chan->message_buffer[0], length);
    }
}

static int acars_correct_errors(uint8_t *octets, unsigned length, uint16_t starting_crc, uint16_t expected_crc)
{
    unsigned i, j;

    // For each octet, check parity
    // If parity is OK, update the CRC and move to the next one
    for (i = 0; i < length && (popcount_table[octets[i]] & 1); ++i) {
        starting_crc = crc_ccitt_update(starting_crc, octets[i]);
    }
    if (i == length) {
        // Means there were no parity errors found before data ended
        return starting_crc == expected_crc;
    } else {
        uint16_t new_crc;
        uint8_t octet = octets[i];
        for (j = 0; j < 8; ++j) {
            // Flip the bit
            octet ^= 1 << j;
            // Update the CRC
            new_crc = crc_ccitt_update(starting_crc, octet);
            // Continue recursively; return if the valid CRC is found
            if (acars_correct_errors(&octets[i+1], length - (i+1), new_crc, expected_crc)) {
                // Apply the found value
                octets[i] = octet;
                return 1;
            }
            // Unflip the bit
            octet ^= 1 << j;
        }
        return 0;
    }
}

static int acars_message_integrity_check(channel_t *chan)
{
    int distance;

    if (chan->received_crc == chan->computed_crc) {
        return 1;
    }
    printf("CRC mismatch detected; computed: %04X, expected: %04X.\n", chan->computed_crc, chan->received_crc);
    printf("Parity error(s) detected: %d\n", chan->parity_errors);
    if (chan->parity_errors == 0) {
        // 1. An error in CRC itself
        distance = hamming_distance(chan->received_crc, chan->computed_crc);
        if (distance > 1) {
            printf("Hamming distance is %d, uncorrectable\n", distance);
        } else {
            printf("Hamming distance is %d, correctable; continuing\n", distance);
            return 1;
        }
        // 2. A double error in an octet, not detected by parity
    } else if (chan->parity_errors <= RECEIVER_CORRECTABLE_ERRORS_MAX) {
        // 1. N bit flips in different bytes; complexity: 8^N message CRC computations
        if (acars_correct_errors(&chan->message_buffer[1], chan->message_index - 1, CRC_CCITT_START, chan->received_crc)) {
            printf("%d error(s) corrected successfully!\n", chan->parity_errors);
            return 1;
        } else {
            printf("Attempts on error correction failed.\n");
        }
    } else {
        // 1. Multiple octets with single bit errors
        printf("Too many errors to be corrected.\n");
    }
    return 0;
}

static int acars_push_octet(receiver_t *recv, channel_t *chan, uint8_t octet)
{
    char ch;
    int distance;

    // Check parity, if needed
    if (!chan->ignore_parity) {
        if (!(popcount_table[octet] & 1)) {
            chan->parity_errors++;
            //printf("WHOA! Parity error! Octet: %02X, pos: %d\n", octet, chan->message_index);
        }
    }

    ch = octet & 0x7F;
    switch (chan->message_state) {
    case ACARS_START:
    case ACARS_EXPECT_SYN1:
        if (ch != ISO5_SYN) {
            // Unexpected character
#if ACARS_VERBOSE
            printf("Unexpected character %02X (expected SYN#1).\n", ch);
#endif
            // See if this is a single bit error in otherwise expected char
            distance = hamming_distance(octet, ISO5_SYN | 0x00);
            if (distance > 1) {
#if ACARS_VERBOSE
                printf("Hamming distance is %d, uncorrectable; RX aborted\n\n", distance);
#endif
                return 0;
            }
#if ACARS_VERBOSE
            printf("Hamming distance is %d, correctable; continuing\n", distance);
#endif
        }
        chan->message_state = ACARS_EXPECT_SYN2;
        break;

    case ACARS_EXPECT_SYN2:
        if (ch != ISO5_SYN) {
            // Unexpected character
#if ACARS_VERBOSE
            printf("Unexpected character %02X (expected SYN#2).\n", ch);
#endif
            // See if this is a single bit error in otherwise expected char
            distance = hamming_distance(octet, ISO5_SYN | 0x00);
            if (distance > 1) {
#if ACARS_VERBOSE
                printf("Hamming distance is %d, uncorrectable; RX aborted\n\n", distance);
#endif
                return 0;
            }
#if ACARS_VERBOSE
            printf("Hamming distance is %d, correctable; continuing\n", distance);
#endif
        }
        chan->message_state = ACARS_EXPECT_SOH;
        break;

    case ACARS_EXPECT_SOH:
        if (ch != ISO5_SOH) {
            // Unexpected character
#if ACARS_VERBOSE
            printf("Unexpected character %02X (expected SOH).\n", ch);
#endif
            // See if this is a single bit error in otherwise expected char
            distance = hamming_distance(octet, ISO5_SOH | 0x00);
            if (distance > 1) {
#if ACARS_VERBOSE
                printf("Hamming distance is %d, uncorrectable; RX aborted\n\n", distance);
#endif
                return 0;
            }
#if ACARS_VERBOSE
            printf("Hamming distance is %d, correctable; continuing\n", distance);
#endif
        }
        chan->message_buffer[chan->message_index++] = octet;
        chan->computed_crc = CRC_CCITT_START;
        chan->message_state = ACARS_EXPECT_STX;
        break;

    case ACARS_EXPECT_STX:
        chan->message_buffer[chan->message_index++] = octet;
        chan->computed_crc = crc_ccitt_update(chan->computed_crc, octet);
        if (chan->message_index >= 14) {
            switch (ch) {
            case ISO5_STX:
                chan->message_state = ACARS_EXPECT_ETX;
                break;
            case ISO5_ETX:
                chan->received_crc = 0;
                chan->ignore_parity = 1;
                chan->message_state = ACARS_EXPECT_BCS1;
                break;
            default:
                /* Unexpected character */
#if ACARS_VERBOSE
                printf("Unexpected character %02X (expected STX/ETX); RX aborted.\n\n", ch);
#endif
                return 0;
            }
        }
        break;

    case ACARS_EXPECT_ETX:
        if (chan->message_index >= sizeof(chan->message_buffer)) {
#if ACARS_VERBOSE
            printf("Message too long; RX aborted.\n\n");
#endif
            return 0;
        }
        chan->message_buffer[chan->message_index++] = octet;
        chan->computed_crc = crc_ccitt_update(chan->computed_crc, octet);
        switch (ch) {
        case ISO5_ETX:
        case ISO5_ETB:
            chan->received_crc = 0;
            chan->ignore_parity = 1;
            chan->message_state = ACARS_EXPECT_BCS1;
            break;
        }
        break;

    case ACARS_EXPECT_BCS1:
        chan->received_crc = octet;
        chan->message_state = ACARS_EXPECT_BCS2;
        break;

    case ACARS_EXPECT_BCS2:
        chan->received_crc |= (uint16_t)octet << 8;
        if (!acars_message_integrity_check(chan)) {
#if ACARS_VERBOSE
            printf("Integrity check failed; RX aborted\n\n");
#endif
            return 0;
        }
        chan->ignore_parity = 0;
        chan->message_state = ACARS_EXPECT_DEL;
        break;

    case ACARS_EXPECT_DEL:
        if (ch != ISO5_DEL) {
            /* Unexpected character */
#if ACARS_VERBOSE
            printf("Unexpected character (expected DEL); error ignored.\n");
#endif
        }
        printf("Message received successfully.\n");
        acars_process_message(recv, chan);
        chan->messages_received_successfully++;
        // Done.
        chan->message_state = ACARS_IDLE;
        return 0;

    default:
        printf("Unexpected state; RX aborted.\n\n");
        return 0;
    }
    return 1;
}

//
// MSK demodulator related functions
//

static int msk_demodulate(channel_t *chan, const real_t *bb_samples)
{
    real_t corr_H0, corr_H1, corr_L0, corr_L1;
    real_t decision_window;
    unsigned i;

    // Compute correlations of the two FSK waveforms over the bit period
    corr_H1 = 0;
    corr_L0 = 0;
    // TODO: use tables.
    for (i = 0; i < RECEIVER_BASEBAND_SAMPLES_PER_BIT; ++i) {
        corr_H1 += bb_samples[i] * sinf(i * R_TWOPI / RECEIVER_BASEBAND_SAMPLES_PER_BIT);
        corr_L0 += bb_samples[i] * sinf(i * R_TWOPI / RECEIVER_BASEBAND_SAMPLES_PER_BIT / 2);
    }
    // Infer the other two coefficients
    corr_H0 = -corr_H1;
    corr_L1 = -corr_L0;
    // Based on the current state, move to the new one and output a bit
    // TODO: Work more on state transitions esp. forbidden ones.
    switch (chan->msk_state) {
    case MSK_STATE_H1:
    case MSK_STATE_L1:
        // Allowed: H1, L0
        decision_window = corr_L0 - corr_H1;
        if (decision_window > 0) {
            // Switch to L0
            chan->msk_state = MSK_STATE_L0;
            return 0;
        } else {
            // Stay in H1
            chan->msk_state = MSK_STATE_H1;
            return 1;
        }
        break;
    case MSK_STATE_H0:
    case MSK_STATE_L0:
        // Allowed: H0, L1
        decision_window = corr_L1 - corr_H0;
        if (decision_window > 0) {
            // Switch to L1
            chan->msk_state = MSK_STATE_L1;
            return 1;
        } else {
            // Stay in H0
            chan->msk_state = MSK_STATE_H0;
            return 0;
        }
        break;
    }
    // Never reached
    return 1;
}

static int channel_compute_phase_adjustment(channel_t *chan, const real_t *bb_samples)
{
    unsigned i;
    // If not in high-freq tone state, return no adjustment
    if (chan->msk_state == MSK_STATE_L1 || chan->msk_state == MSK_STATE_L0) {
        return 0;
    }
    // Locate the zero crossing
    for (i = RECEIVER_BASEBAND_SAMPLES_PER_BIT / 4; i < 3 * RECEIVER_BASEBAND_SAMPLES_PER_BIT / 4; ++i) {
        if ((bb_samples[i] <= 0 && bb_samples[i+1] >= 0) || (bb_samples[i] <= 0 && bb_samples[i+1] >= 0)) {
            return i - RECEIVER_BASEBAND_SAMPLES_PER_BIT / 2;
        }
    }
    // Somehow failed to locate the zero crossing!
    return 0;
    // Another idea: 
    // - if both middle samples are above, step forward; 
    // - if below, step backward
    // - do nothing otherwise
}

//
// Channel states implementation
//

// Returns the estimate of the phase of an expected 2400Hz tone in the provided buffer
// Returns a normalised phasor representing the phase offset
// Assuming that the (real) input is w(t)=(wt+phi), then the spectrum is composed of +w(t), -w(t), and DC.
// If we mix that with +w(t), then:
// exp(jw(t)) * (A*exp(jw(t)+phi) + A*exp(-(jw(t)+phi)) + DC*exp(0)) =
// exp(jw(t)) * A*exp(jw(t)+phi) + exp(jw(t)) * A*exp(-jw(t)-phi) + exp(jw(t)) * DC*exp(0) =
// A*exp(jw(t)+jw(t)+phi) + A*exp(jw(t)-jw(t)-phi) + DC * exp(jw(t)+0) =
// A*exp(2*jw(t)+phi) + A*exp(-phi) + DC * exp(jw(t))
// By integrating this over a single period, terms with jw(t) will cancel out
// due to these integrals being zero. The only term that remains would be A*exp(-phi).
// NOTE: Investigate Goertzel's algorithm for this.
// https://en.wikipedia.org/wiki/Goertzel_algorithm
static complex_t compute_phasor_estimate(const real_t *x)
{
    unsigned i;
    complex_t w, dw;
    complex_t result;

    // TODO: optimize: these are constants.
    dw.re = cosf(-R_TWOPI / RECEIVER_BASEBAND_SAMPLES_PER_BIT);
    dw.im = sinf(-R_TWOPI / RECEIVER_BASEBAND_SAMPLES_PER_BIT);
    result.re = result.im = _R(0);
    w.re = _R(1); w.im = _R(0);
    for (i = 0; i < RECEIVER_BASEBAND_SAMPLES_PER_BIT; ++i) {
        // NOTE: complex multiplication simplifies due to one value being a real
        result.re += x[i] * w.re;
        result.im += x[i] * w.im;
        {
            real_t tre = w.re, tim = w.im;
            w.re = tre * dw.re - tim * dw.im;
            w.im = tre * dw.im + tim * dw.re;
        }
    }
    // Since we computed A*exp(-phi), invert the sign to get A*exp(phi)
    // NOTE: can also move this into the loop above, replacing addition with subtraction
    result.im = -result.im;
    return result;
}

// Returns the estimate of the phase of an expected 2400Hz tone in the provided buffer
// Return range: [-1, 1] for [-pi, pi]
static real_t compute_phase_angle_estimate(const real_t *x)
{
    complex_t result = compute_phasor_estimate(x);
    return R_1_PI * atan2f(result.im, result.re);
}

// Compute the mean and variance values for a given sequence
static void compute_statistics(const real_t *data, unsigned length, real_t *average, real_t *variance)
{
    unsigned i;
    real_t avg = 0;
    real_t var = 0;

    for (i = 0; i < length; ++i) {
        avg += data[i];
    }
    avg /= length;
    *average = avg;
    for (i = 0; i < length; ++i) {
        real_t d;
        d = data[i] - avg;
        var += d * d;
    }
    var /= length - 1;
    *variance = var;
}

static void compute_linear_regression(const real_t *samples, unsigned length, real_t *a0, real_t *a1)
{
    unsigned n;
    real_t sy, syy;
    real_t sxy;
    real_t sx, sxx;
    real_t b;

    /* https://en.wikipedia.org/wiki/Simple_linear_regression#Fitting_the_regression_line */

    sxy = syy = sy = 0;
    for (n = 0; n < length; ++n) {
        real_t y = samples[n];

        sy += y;
        syy += y * y;
        sxy += n * y;
    }
    /* http://www.trans4mind.com/personal_development/mathematics/index.html */
    sx = (real_t)n * n / 2.0f + (real_t)n / 2.0f;
    sxx = (real_t)n * n * n / 3.0f + (real_t)n * n / 2.0f + (real_t)n / 6.0f;

    b = (n * sxy - sx * sy) / (n * sxx - sx * sx);
    *a1 = b;
    *a0 = sy / n - b * sx / n;
}

static void channel_state_enter_prekey_detect(channel_t *chan);
static void channel_state_enter_prekey_measure(channel_t *chan);
static void channel_state_enter_bit_sync(channel_t *chan);
static void channel_state_enter_receive(channel_t *chan);

static void channel_abort(channel_t *chan)
{
    channel_state_enter_prekey_detect(chan);
    // NOTE: Move somewhere more appropriate
    printf("CH %u: RX Triggered: %d; RX Successful: %d\n", chan->fc,
        chan->message_prekey_detected,
        chan->messages_received_successfully);
}


static void channel_state_enter_prekey_detect(channel_t *chan)
{
    chan->state = CHANNEL_STATE_PREKEY_DETECT;
    chan->phase_estimates_index = 0;
    chan->phase_estimates_ok = 0;
#ifdef DEBUG_DUMP_CSV_DATA
    if (chan->fp_debug_csv) {
        fclose(chan->fp_debug_csv);
        chan->fp_debug_csv = NULL;
    }
#endif
}

static unsigned channel_state_prekey_detect(channel_t *chan, const real_t *bb_samples)
{
    unsigned index;
    real_t phase;
    real_t average_zero, variance_zero;
    real_t average_pi, variance_pi;

    // Measure the phase angle
    phase = compute_phase_angle_estimate(bb_samples);
    // Collect estimates to do statistics on later
    // NOTE: Due to wraparound issues, collect phi+0 and phi+pi phases
    index = chan->phase_estimates_index;
    chan->phase_estimates[index] = phase;
    chan->phase_estimates_pi[index] = phase > _R(0) ? phase - _R(1) : phase + _R(1);
    if (index == RECEIVER_PHASE_ESTIMATE_COUNT - 1) {
        chan->phase_estimates_ok = 1;
        index = 0;
    } else {
        index++;
    }
    chan->phase_estimates_index = index;
    // Make sure there are enough samples
    if (!chan->phase_estimates_ok) {
        return RECEIVER_BASEBAND_SAMPLES_PER_BIT;
    }
    compute_statistics(chan->phase_estimates, RECEIVER_PHASE_ESTIMATE_COUNT, &average_zero, &variance_zero);
    compute_statistics(chan->phase_estimates_pi, RECEIVER_PHASE_ESTIMATE_COUNT, &average_pi, &variance_pi);
    // Check whether the variance is below the threshold
    if (variance_zero > _R(0.04) && variance_pi > _R(0.04)) {
        return RECEIVER_BASEBAND_SAMPLES_PER_BIT;
    }
    // Switch to the next state
    channel_state_enter_prekey_measure(chan);
    chan->message_prekey_detected++;
    // Can adjust the phase roughly as well
    if (variance_zero < variance_pi) {
        return RECEIVER_BASEBAND_SAMPLES_PER_BIT;
    } else {
        return RECEIVER_BASEBAND_SAMPLES_PER_BIT / 2;
    }
}


static void channel_state_enter_prekey_measure(channel_t *chan)
{
    char buffer[256];
    chan->state = CHANNEL_STATE_PREKEY_MEASURE;
    chan->phase_estimates_index = 0;
#ifdef DEBUG_DUMP_CSV_DATA
    sprintf(buffer, "chan.%u.%d.csv", chan->fc, time(NULL));
    chan->fp_debug_csv = fopen(buffer, "w");
#endif
}

static unsigned channel_state_prekey_measure(channel_t *chan, const real_t *bb_samples)
{
    real_t phase;
    real_t average, variance;
    real_t phase_shift;
    int sample_shift;

    // Measure the phase angle
    // NOTE: The bit interval starts on phase angle of 90 deg.
    phase = compute_phase_angle_estimate(bb_samples) - _R(0.5);
    // I wonder if this ever happens...
    while (phase <= _R(-1)) {
        phase += _R(2);
    }
    // Collect estimates to do statistics on later
    chan->phase_estimates[chan->phase_estimates_index] = phase;
    chan->phase_estimates_index++;
    if (chan->phase_estimates_index < RECEIVER_PHASE_ESTIMATE_COUNT) {
        return RECEIVER_BASEBAND_SAMPLES_PER_BIT;
    }

    compute_statistics(chan->phase_estimates, RECEIVER_PHASE_ESTIMATE_COUNT, &average, &variance);
    // TODO: linear regression code is borken. Figure out what's wrong.
    // compute_linear_regression(chan->phase_estimates, RECEIVER_PHASE_ESTIMATE_COUNT, &phase_a0, &phase_a1);
    // Switch to the next state
    channel_state_enter_bit_sync(chan);
#ifdef DEBUG_DUMP_CSV_DATA
    // Mark this in the debug CSV output
    if (chan->fp_debug_csv) {
        fprintf(chan->fp_debug_csv, "\n\n");
    }
#endif
    phase_shift = -average * _R(0.5) * RECEIVER_BASEBAND_SAMPLES_PER_BIT;
    // Using phase angle, adjust the bit interval start
    if (phase_shift > 0) {
        // Sampling too late
        sample_shift = RECEIVER_BASEBAND_SAMPLES_PER_BIT - phase_shift;
    } else {
        // Sampling too early
        sample_shift = 0 - phase_shift;
    }
    //printf("CH %u: measurements complete\n", chan->fc);
    printf("CH %u: DC %+4.2fdB, phase offset %+.6f => bit interval %d\n", chan->fc, 20.0f * log10f(chan->dc), average, sample_shift);
    return sample_shift;
}


static void channel_state_enter_bit_sync(channel_t *chan)
{
    chan->state = CHANNEL_STATE_BIT_SYNC;
    chan->phase_estimates_index = 0;
    chan->msk_state = MSK_STATE_H1;
    chan->sync_reg = 0xFFFF;
    chan->sync_counter = 0;

    chan->dc_measurements_index = 0;
}

static unsigned channel_state_bit_sync(channel_t *chan, const real_t *bb_samples)
{
    unsigned bit;

    // Estimate the current bit
    bit = msk_demodulate(chan, bb_samples);
    // Shift in
    chan->sync_reg = (bit << 15) | (chan->sync_reg >> 1);
    // TODO: figure out how to make use of parity bits to detect/correct errors here.
    if (chan->sync_reg == 0x2AAB) {
        printf("CH %u: bit sync achieved after %d bits\n", chan->fc, chan->sync_counter - 16);
        channel_state_enter_receive(chan);
#ifdef DEBUG_DUMP_CSV_DATA
        // Mark this in the debug CSV output
        if (chan->fp_debug_csv) {
            fprintf(chan->fp_debug_csv, "\n\n");
        }
#endif
    } else {
        chan->sync_counter++;
        // Some sane default...
        if (chan->sync_counter > 460) {
            printf("CH %u: no bit sync achieved within sane time; RX aborted\n", chan->fc);
            channel_abort(chan);
        }
    }
    return RECEIVER_BASEBAND_SAMPLES_PER_BIT + channel_compute_phase_adjustment(chan, bb_samples);
}


static void channel_state_enter_receive(channel_t *chan)
{
    chan->state = CHANNEL_STATE_RECEIVE;
    chan->bits_in_reg = 0;
    chan->ignore_parity = 0;
    chan->parity_errors = 0;
    chan->message_state = ACARS_START;
    chan->message_index = 0;
}

static unsigned channel_state_receive(receiver_t *recv, channel_t *chan, const real_t *bb_samples)
{
    unsigned bit;

    // Estimate the current bit
    bit = msk_demodulate(chan, bb_samples);
    // Shift in and increment valid bit count
    chan->shift_reg = (bit << 7) | (chan->shift_reg >> 1);
    chan->bits_in_reg++;
    // Might be that we have an octet already
    if (chan->bits_in_reg >= 8) {
        if (!acars_push_octet(recv, chan, chan->shift_reg)) {
            channel_abort(chan);
        }
        chan->bits_in_reg = 0;
    }
    return RECEIVER_BASEBAND_SAMPLES_PER_BIT + channel_compute_phase_adjustment(chan, bb_samples);
}

//
// Actual DSP implementation
//

static void channel_process_am_baseband(receiver_t *recv, channel_t *chan)
{
    unsigned index;
    unsigned advance = RECEIVER_BASEBAND_SAMPLES_PER_BIT;

    // Process all the baseband bit intervals available
    for (index = 0; (index + 2 * RECEIVER_BASEBAND_SAMPLES_PER_BIT) < chan->baseband_am_index; index += advance) {
#ifdef DEBUG_DUMP_CSV_DATA
        if (chan->fp_debug_csv) {
            write_as_csv(&chan->baseband_am[index], RECEIVER_BASEBAND_SAMPLES_PER_BIT, chan->fp_debug_csv);
        }
#endif
        switch (chan->state) {
        case CHANNEL_STATE_PREKEY_DETECT:
            advance = channel_state_prekey_detect(chan, &chan->baseband_am[index]);
            break;
        case CHANNEL_STATE_PREKEY_MEASURE:
            advance = channel_state_prekey_measure(chan, &chan->baseband_am[index]);
            break;
        case CHANNEL_STATE_BIT_SYNC:
            advance = channel_state_bit_sync(chan, &chan->baseband_am[index]);
            break;
        case CHANNEL_STATE_RECEIVE:
            advance = channel_state_receive(recv, chan, &chan->baseband_am[index]);
            break;
        }
    }
    // Preserve the remainder, if any
    memmove(&chan->baseband_am[0], &chan->baseband_am[index], sizeof(real_t) * (chan->baseband_am_index - index));
    chan->baseband_am_index -= index;
}

static real_t channel_dc_removal(channel_t *chan, real_t input)
{
    real_t dc;
    unsigned i;

    i = chan->dc_measurements_index;
    chan->dc_measurements[i] = input;
#if 0
    if (i == RECEIVER_DC_MEASUREMENTS_MAX - 1) {
        chan->dc_measurements_index = 0;
    } else {
        chan->dc_measurements_index = i + 1;
    }

    dc = 0;
    for (i = 0; i < RECEIVER_DC_MEASUREMENTS_MAX; ++i) {
        dc += chan->dc_measurements[i];
    }
    dc /= RECEIVER_DC_MEASUREMENTS_MAX;
    chan->dc = dc;
#else
    if (i == RECEIVER_DC_MEASUREMENTS_MAX - 1) {
        chan->dc_measurements_index = 0;
        dc = 0;
        for (i = 0; i < RECEIVER_DC_MEASUREMENTS_MAX; ++i) {
            dc += chan->dc_measurements[i];
        }
        dc /= RECEIVER_DC_MEASUREMENTS_MAX;
        chan->dc = dc;
    } else {
        chan->dc_measurements_index = i + 1;
        dc = chan->dc;
    }

#endif

    return input - dc;
}

static void channel_process_if(receiver_t *recv, channel_t *chan, unsigned count)
{
    unsigned i;
    complex_t ddc_w, ddc_dw;
    unsigned if_buffer_index;
    unsigned limit;

    // Run DDC stage
    if_buffer_index = chan->if_buffer_index;
    ddc_w = chan->ddc_w;
    ddc_dw = chan->ddc_dw;
    for (i = 0; i < count; ++i) {
        real_t tmp_re, tmp_im;
        complex_t x = recv->if_buffer[i];
        // DDC mixing
        chan->if_buffer[if_buffer_index].re = x.re * ddc_w.re - x.im * ddc_w.im;
        chan->if_buffer[if_buffer_index].im = x.re * ddc_w.im + x.im * ddc_w.re;
        if_buffer_index++;
        // Update DDC state
        tmp_re = ddc_w.re; tmp_im = ddc_w.im;
        ddc_w.re = tmp_re * ddc_dw.re - tmp_im * ddc_dw.im;
        ddc_w.im = tmp_re * ddc_dw.im + tmp_im * ddc_dw.re;
    }
    // Normalize the length of the DDC phasor
    // Due to multiplicative errors, the length drifts
    {
        real_t n = _R(1) / hypotf(ddc_w.re, ddc_w.im);
        ddc_w.re *= n;
        ddc_w.im *= n;
    }
    chan->ddc_w = ddc_w;

    // Run decimation and AM demod stage
    for (i = 0; i < if_buffer_index; i += recv->baseband_decimate_rate) {
        complex_t x;
        real_t A;
        x = fir_convolve_symmetric_cr(&chan->if_buffer[i], recv->if_decim_filter, recv->if_decim_filter_length);
        // AM demodulate
        A = hypotf(x.re, x.im);
        chan->baseband_am_unfiltered[chan->baseband_am_unfiltered_index++] = A;
    }
    // Preserve the remainder
    i -= recv->baseband_decimate_rate;
    if_buffer_index -= i;
    memmove(&chan->if_buffer[0], &chan->if_buffer[i], if_buffer_index * sizeof(chan->if_buffer[0]));
    chan->if_buffer_index = if_buffer_index;

    // Run baseband FIR and DC removal
    if (chan->baseband_am_unfiltered_index < recv->baseband_filter_length) {
        return;
    }
    limit = chan->baseband_am_unfiltered_index - recv->baseband_filter_length;
    for (i = 0; i < limit; ++i) {
        real_t A;
        A = fir_convolve_symmetric_rr(&chan->baseband_am_unfiltered[i], recv->baseband_filter, recv->baseband_filter_length);
        A = channel_dc_removal(chan, A);
        chan->baseband_am[chan->baseband_am_index++] = A;
    }
    chan->baseband_am_unfiltered_index -= i;
    memmove(&chan->baseband_am_unfiltered[0], &chan->baseband_am_unfiltered[i], chan->baseband_am_unfiltered_index * sizeof(chan->baseband_am_unfiltered[0]));

    channel_process_am_baseband(recv, chan);
}

static void receiver_process_if(receiver_t *recv, unsigned count)
{
    unsigned channel_id;
    // Dispatch processing for each channel
    // TODO: task management to spread work over cores?
    for (channel_id = 0; channel_id < recv->channel_count; ++channel_id) {
        channel_process_if(recv, &recv->channels[channel_id], count);
    }
}

static void receiver_process_samples_u8u8(receiver_t *recv, const uint8_t *buf, unsigned count)
{
    unsigned i;
    for (i = 0; i < count; i++) {
        recv->if_buffer[i].re = ((int)buf[i * 2 + 0] - 127) * (1.0f / 128.0f);
        recv->if_buffer[i].im = ((int)buf[i * 2 + 1] - 127) * (1.0f / 128.0f);
    }
    receiver_process_if(recv, count);
}

//
// Initialization, teardown, and configuration stuff
//

static void receiver_init_channel(receiver_t *recv, unsigned channel_id, unsigned fc)
{
    channel_t *chan = &recv->channels[channel_id];
    real_t dw;
    char fname[256];

    // NOTE: allocated by calloc(), no need to set zeros
    chan->fc = fc;
    chan->if_buffer = (complex_t *)calloc(RECEIVER_IF_BLOCK_LENGTH * 2, sizeof(complex_t));
    chan->ddc_w.re = _R(1);
    chan->ddc_w.im = _R(0);
    dw = R_TWOPI * ( _R(fc) - _R(recv->fc)) / _R(recv->fs);
    chan->ddc_dw.re = cosf(-dw);
    chan->ddc_dw.im = sinf(-dw);
    chan->baseband_am_unfiltered = (real_t *)calloc(RECEIVER_IF_BLOCK_LENGTH * 2, sizeof(real_t));
    chan->baseband_am = (real_t *)calloc(RECEIVER_IF_BLOCK_LENGTH * 2, sizeof(real_t));

#ifdef DEBUG_DUMP_CSV_DATA
    chan->fp_debug_csv = NULL;
#endif

    channel_state_enter_prekey_detect(chan);
}

static void receiver_init(receiver_t *recv, unsigned channel_count, unsigned fc, unsigned fs)
{
    real_t f_cutoff;

    recv->channel_count = channel_count;
    recv->channels = (channel_t *)calloc(channel_count, sizeof(channel_t));
    recv->fc = fc;
    recv->fs = fs;
    recv->if_buffer = (complex_t *)malloc(RECEIVER_IF_BLOCK_LENGTH * sizeof(complex_t));
    recv->baseband_decimate_rate = fs / RECEIVER_BASEBAND_SAMPLE_RATE;
    // Generate the decimation FIR coefficients
    recv->if_decim_filter_length = (recv->baseband_decimate_rate * 3) | 1;
    recv->if_decim_filter = (real_t *)calloc(recv->if_decim_filter_length, sizeof(real_t));
    f_cutoff = _R(RECEIVER_IF_FILTER_CUTOFF) / _R(fs);
    fir_generate_sinc_f32(f_cutoff, recv->if_decim_filter_length, recv->if_decim_filter);
    fir_apply_window_f32(recv->if_decim_filter_length, recv->if_decim_filter, window_blackman);
    fir_normalize_f32(recv->if_decim_filter_length, recv->if_decim_filter);
    // Generate the baseband FIR coefficients
    // TODO: experiment with filter shapes
    recv->baseband_filter_length = RECEIVER_BASEBAND_OVERSAMPLE | 1;
    recv->baseband_filter = (real_t *)calloc(recv->baseband_filter_length, sizeof(real_t));
    f_cutoff = _R(RECEIVER_BASEBAND_FILTER_CUTOFF) / _R(RECEIVER_BASEBAND_SAMPLE_RATE);
    fir_generate_sinc_f32(f_cutoff, recv->baseband_filter_length, recv->baseband_filter);
    fir_apply_window_f32(recv->baseband_filter_length, recv->baseband_filter, window_blackman);
    fir_normalize_f32(recv->baseband_filter_length, recv->baseband_filter);
}

//
// RTL-SDR dongle support
//

static rtlsdr_dev_t *rtlsdr_init(int device_index, unsigned sample_rate, unsigned fc, float gain, int ppm_correction)
{
    int rv;
    rtlsdr_dev_t *dev;

    rv = rtlsdr_open(&dev, device_index);
    if (rv) {
        fprintf(stderr, "Failed to open the device #%d\n", device_index);
        return NULL;
    }
    rv = rtlsdr_set_freq_correction(dev, ppm_correction);
    if (rv && rv != -2) {
        fprintf(stderr, "Failed to set the frequency correction\n");
        return NULL;
    }
    // fXTAL = 28.8M
    // fXTAL / fT = 28.8M / 2.4k = 12000
    rv = rtlsdr_set_sample_rate(dev, sample_rate);
    if (rv) {
        fprintf(stderr, "Failed to set the sample rate\n");
        return NULL;
    }
    rv = rtlsdr_set_center_freq(dev, fc);
    if (rv) {
        fprintf(stderr, "Failed to set the center frequency\n");
        return NULL;
    }
    rv = rtlsdr_set_offset_tuning(dev, 0);
#if 0
    if (rv) {
        fprintf(stderr, "Failed to disable offset tuning\n");
        return NULL;
    }
#endif
    rv = rtlsdr_set_if_freq(dev, 0);
#if 0
    if (rv) {
        fprintf(stderr, "Failed to set IF frequency\n");
        return NULL;
    }
#endif
    rv = rtlsdr_set_direct_sampling(dev, 0);
    if (rv) {
        fprintf(stderr, "Failed to disable direct sampling\n");
        return NULL;
    }
    rv = rtlsdr_set_agc_mode(dev, 0);
    if (rv) {
        fprintf(stderr, "Failed to disable AGC\n");
        return NULL;
    }
    rv = rtlsdr_set_tuner_gain_mode(dev, 1);
    if (rv) {
        fprintf(stderr, "Failed to switch to manual gain\n");
        return NULL;
    }
    rv = rtlsdr_set_tuner_gain(dev, (int)(gain * 10));
    if (rv) {
        int gain_count;
        int *gains;
        int i;

        gain_count = rtlsdr_get_tuner_gains(dev, NULL);
        gains = (int *)malloc(gain_count * sizeof(int));
        rtlsdr_get_tuner_gains(dev, gains);
        fprintf(stderr, "Failed to set the gain value; please 2x check that you used one of the following gain settings:\n");
        for (i = 0; i < gain_count; ++i) {
            fprintf(stderr, "  %.1f\n", gains[i] * 0.1f);
        }
        free(gains);
        return NULL;
    }

    rv = rtlsdr_reset_buffer(dev);
    if (rv) {
        fprintf(stderr, "Failed to reset buffers\n");
        return NULL;
    }
    return dev;
}

static void rtlsdr_callback(const uint8_t *buf, uint32_t len, void *ctx)
{
    receiver_t *recv = (receiver_t *)ctx;
    uint32_t iq_samples = len >> 1;
    double start_wc, end_wc;
    static double last_start_wc;

#ifdef DEBUG_WRITE_RAW_IF
    fwrite(buf, 1, len, recv->raw_fp);
#endif
    start_wc = get_wall_time();
    receiver_process_samples_u8u8(recv, buf, iq_samples);
    end_wc = get_wall_time();
    //printf("Tick: %lf; Processing: %03d\n", start_wc - last_start_wc, (int)(100 * (end_wc - start_wc) / (start_wc - last_start_wc)));
    last_start_wc = start_wc;
}

static int rtlsdr_run(rtlsdr_dev_t *dev, receiver_t *recv)
{
    int rv;

    rv = rtlsdr_read_async(dev, (rtlsdr_read_async_cb_t)rtlsdr_callback, recv, 8, RECEIVER_IF_BLOCK_LENGTH);
    if (rv) {
        fprintf(stderr, "Failed to start sampling\n");
        return 0;
    }
    // The above doesn't actually return until someone calls the cancel function.
    return 1;
}

static void rtlsdr_mainloop(receiver_t *recv, int device_index, unsigned sample_rate, unsigned fc, float gain, int ppm_correction)
{
    rtlsdr_dev_t *dev;

    dev = rtlsdr_init(device_index, sample_rate, fc, gain, ppm_correction);
    if (!dev) {
        return;
    }
    if (!rtlsdr_run(dev, recv)) {
        return;
    }
}

//
// File based IQ input support
//

static void iqfile_mainloop(receiver_t *recv, const char *path)
{
    uint8_t *buffer;
    size_t buffer_size;
    FILE *fp;

    buffer_size = RECEIVER_IF_BLOCK_LENGTH * 2;
    buffer = (uint8_t *)malloc(buffer_size);
    fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "Could not open the input file\n");
        return;
    }
    while (!feof(fp)) {
        size_t count;
        count = fread(buffer, 1, buffer_size, fp);
        receiver_process_samples_u8u8(recv, buffer, count / 2);
    }
    fclose(fp);
    free(buffer);
}

//
// The main()
//

static void print_usage(const char *program_path)
{
    fprintf(stderr, "Usage: %s [options] fCenter fChannel1 [fChannel2] ...\n\n", program_path);
    fprintf(stderr, "Frequencies are specified as integers, in kHz.\n\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -g gain\n\tSet the dongle's gain to the given value in dB; default: 42.1\n");
    fprintf(stderr, "  -p ppm\n\tSet the dongle's freq correction to the given value, in ppm; deafult: 0\n");
    fprintf(stderr, "  -r index\n\tReceive via RTL-SDR dongle with given index\n");
    fprintf(stderr, "  -q path\n\tReceive via an IQ file with the given path\n");
    fprintf(stderr, "  -S\n\tOutput received messages on the screen\n");
    fprintf(stderr, "  -L path\n\tAppend received messages to the log file\n");
    fprintf(stderr, "  -U host:port\n\tSend received messages to the given address via UDP\n");
    fprintf(stderr, "  -I text\n\tSet the station's ID for UDP reporting\n");
}

typedef enum _input_source_t {
    INPUT_SOURCE_UNDEFINED,
    // Working
    INPUT_SOURCE_RTLSDR,
    INPUT_SOURCE_IQFILE,
} input_source_t;

#ifdef _WIN32
void win32_socket_init(void)
{
    WSADATA wsa;
    WSAStartup(MAKEWORD(2,0), &wsa);
}
#endif

int main(int argc, char *argv[], char *envp[])
{
    receiver_t rx;
    int option;
    input_source_t source = INPUT_SOURCE_UNDEFINED;
    int rtlsdr_device_index = -1;
    int ppm_correction = 0;
    unsigned fc = 131600000;
    float gain = 42.1f;
    int channel_count;
    int index;
    int bb_to_if_sample_rate_factor = 25;
    unsigned if_sample_rate;
    int screen_output_enabled = 0;
    char *udp_destination = NULL;
    char *log_destination = NULL;
    char *station_id = NULL;
    char *iq_path;

#ifdef _WIN32
    win32_socket_init();
#endif

    fprintf(stderr, "DJ's ACARS receiver-decoder, version 0.9\n\n");

    // Parse option arguments
    do {
        option = getopt(argc, argv, "vg:p:r:q:SL:U:I:");
        switch (option) {
        case 'g':
            // Gain, in dB
            gain = (float)atof(optarg);
            break;
        case 'p':
            // PPM correction to be applied
            ppm_correction = strtol(optarg, NULL, 10);
            break;
        // Input options
        case 'r':
            // RTL-SDR device index
            source = INPUT_SOURCE_RTLSDR;
            rtlsdr_device_index = strtoul(optarg, NULL, 10);
            break;
        case 'q':
            source = INPUT_SOURCE_IQFILE;
            iq_path = optarg;
            break;
        // Output options
        case 'S':
            screen_output_enabled = 1;
            break;
        case 'L':
            log_destination = optarg;
            break;
        case 'U':
            udp_destination = optarg;
            break;
        case 'I':
            station_id = optarg;
            break;
        case EOF:
            break;
        }
    } while (option != -1);

    if (source == INPUT_SOURCE_UNDEFINED) {
        fprintf(stderr, "No input source specified.\n");
        print_usage(argv[0]);
        return 1;
    }

    // TODO: figure out the optimal BB to IF sample rate ratio
    // Hardcoded for now.
    if_sample_rate = RECEIVER_BASEBAND_SAMPLE_RATE * bb_to_if_sample_rate_factor;

    // Expect the rest of arguments to be frequencies
    if (argc - optind < 2) {
        // Need at least two more options
        fprintf(stderr, "Not enough frequencies specified.\n");
        print_usage(argv[0]);
        return 1;
    }
    fc = strtoul(argv[optind], NULL, 10) * 1000;
    channel_count = argc - optind - 1;
    receiver_init(&rx, channel_count, fc, if_sample_rate);
    for (index = 0; index < channel_count; ++index) {
        unsigned fchan = strtoul(argv[optind + 1 + index], NULL, 10) * 1000;
        receiver_init_channel(&rx, index, fchan);
    }
    // Handle output options
    rx.screen_output_enabled = screen_output_enabled;
    if (log_destination) {
        rx.output_log_fp = fopen(log_destination, "ab");
        if (!rx.output_log_fp) {
            rx.output_log_fp = fopen(log_destination, "wb");
        }
        if (!rx.output_log_fp) {
            fprintf(stderr, "Failed to open the log file for writing!\n");
        }
    } else {
        rx.output_log_fp = NULL;
    }
    if (udp_destination) {
        struct addrinfo *pai;
        char *port;

        port = strrchr(udp_destination, ':');
        if (port) {
            *port = '\0';
            ++port;
        } else {
            port = "13928";
        }
        if (getaddrinfo(udp_destination, port, NULL, &pai)) {
            fprintf(stderr, "Failed to resolve address: %s\n", udp_destination);
        } else {
            struct addrinfo *ptr;
            struct sockaddr *addr;

            for (ptr = pai; ptr; ptr = ptr->ai_next) {
                if (ptr->ai_family == AF_INET) {
                    break;
                }
            }
            if (ptr) {
                rx.output_socket_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
                if (connect(rx.output_socket_fd, ptr->ai_addr, ptr->ai_addrlen)) {
                    fprintf(stderr, "Calling connect() failed\n");
                }
            } else {
                fprintf(stderr, "No addresses returned for: %s\n", udp_destination);
            }
        }
    } else {
        rx.output_socket_fd = -1;
    }

    memset(rx.station_id, '\0', sizeof(rx.station_id));
    if (station_id) {
        size_t len = strlen(station_id);
        memcpy(rx.station_id, station_id, len > 8 ? 8 : len);
    }

    switch (source) {
    case INPUT_SOURCE_RTLSDR:
        rtlsdr_mainloop(&rx, rtlsdr_device_index, if_sample_rate, fc, gain, ppm_correction);
        break;
    case INPUT_SOURCE_IQFILE:
        iqfile_mainloop(&rx, iq_path);
        break;
    }

    return 0;
}
