VPATH = src
CFLAGS = -O2 -mcpu=cortex-a53 -mfpu=neon-fp-armv8 -mfloat-abi=hard  -funsafe-math-optimizations
LDFLAGS = -lm -lrtlsdr

all: spycars-vhf

spycars-vhf: spycars-vhf.o 

clean:
	rm -f *.o spycars-vhf
