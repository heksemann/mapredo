#include <iostream>
#include <inttypes.h>

#include "base.h"

#define P07 10000000LLU
#define P08 100000000LLU
#define P09 1000000000LLU
#define P10 10000000000LLU
#define P11 100000000000LLU
#define P12 1000000000000LLU

static inline size_t digits10 (uint64_t v)
{
    if (v < 10) return 1;
    if (v < 100) return 2;
    if (v < 1000) return 3;
    if (v < P12)
    {
	if (v < P08)
	{
	    if (v < 1000000)
	    {
		if (v < 10000) return 4;
		return 5 + (v >= 100000);
	    }
	    return 7 + (v >= P07);
	}

	if (v < P10) return 9 + (v >= P09);
	return 11 + (v >= P11);
    }
    return 12 + digits10 (v/P12);
}

size_t
mapredo::base::uint_to_ascii (uint64_t value, char* dst)
{
    static const char digits[] = \
	"0001020304050607080910111213141516171819"
	"2021222324252627282930313233343536373839"
	"4041424344454647484950515253545556575859"
	"6061626364656667686970717273747576777879"
	"8081828384858687888990919293949596979899";
    size_t const length = digits10(value);
    size_t next = length - 1;

    while (value >= 100)
    {
	uint64_t const i = (value % 100) * 2;
	value /= 100;
	dst[next] = digits[i + 1];
	dst[next - 1] = digits[i];

	next -= 2;
    }

    // Handle last 1-2 digits
    if (value < 10) dst[next] = '0' + uint32_t(value);
    else
    {
	uint32_t i = uint32_t(value) * 2;
	dst[next] = digits[i + 1];
	dst[next - 1] = digits[i];
    }

    return length;
}

#if 0
int main()
{
    std::cerr << "Digits " << digits10(1) << "\n";
    std::cerr << "Digits " << digits10(22) << "\n";
    std::cerr << "Digits " << digits10(333) << "\n";
    std::cerr << "Digits " << digits10(4444) << "\n";
    std::cerr << "Digits " << digits10(55555) << "\n";
    std::cerr << "Digits " << digits10(666666) << "\n";
    std::cerr << "Digits " << digits10(7777777) << "\n";
    std::cerr << "Digits " << digits10(88888888) << "\n";
    std::cerr << "Digits " << digits10(999999999) << "\n";
    std::cerr << "Digits " << digits10(1000000000LLU) << "\n";
    std::cerr << "Digits " << digits10(10000000000LLU) << "\n";
    std::cerr << "Digits " << digits10(100000000000LLU) << "\n";
    std::cerr << "Digits " << digits10(1000000000000LLU) << "\n";
    std::cerr << "Digits " << digits10(10000000000000LLU) << "\n";

    char res[10];
    unsigned int len;

    len = uint_to_ascii (100, res);
    std::cerr << std::string(res, len) << "\n";
    len = uint_to_ascii (123456789, res);
    std::cerr << std::string(res, len) << "\n";
    len = uint_to_ascii (123456890123LL, res);
    std::cerr << std::string(res, len) << "\n";
}
#endif
