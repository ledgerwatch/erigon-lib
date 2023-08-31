package rlp

type Token byte

func (T Token) Plus(n byte) byte {
	return byte(T) + n
}

// This token table can also be used for offsets. how cool!
const (
	TokenDecimal     Token = 0x00
	TokenShortString Token = 0x80
	TokenLongString  Token = 0xb7
	TokenShortList   Token = 0xc0
	TokenLongList    Token = 0xf7

	TokenUnknown Token = 0xff
)

func identifyToken(b byte) Token {
	switch {
	case b >= 0 && b <= 127:
		return TokenDecimal
	case b >= 128 && b <= 183:
		return TokenShortString
	case b >= 184 && b <= 191:
		return TokenLongString
	case b >= 192 && b <= 247:
		return TokenShortList
	case b >= 248 && b <= 255:
		return TokenLongList
	}
	return TokenUnknown
}
