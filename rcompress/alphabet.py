
# LITERAL AND MATCH LENGTH CODES

#      Extra               Extra               Extra

# Code Bits Length(s) Code Bits Lengths   Code Bits Length(s)

# ---- ---- ------     ---- ---- -------   ---- ---- -------

#  257   0     4       267   1   17,18     277   4   83-98

#  258   0     5       268   2   19-22     278   4   99-114

#  259   0     6       269   2   23-26     279   4   115-130

#  260   0     7       270   2   27-30     280   5   131-162

#  261   0     8       271   2   31-34     281   5   163-194

#  262   0     9       272   3   35-42     282   5   195-226

#  263   0    10       273   3   43-50     283   5   227-255

#  264   1  11,12      274   3   51-58

#  265   1  13,14      275   3   59-66

#  266   1  15,16      276   4   67-82


# 284 alphabet size


def generate_match_len_codes():

    codes = []

    assert(12-11 == 0b1)
    assert(14-13 == 0b1)
    assert(16-15 == 0b1)
    assert(18-17 == 0b1)

    assert(22-19 == 0b11)
    assert(26-23 == 0b11)
    assert(30-27 == 0b11)
    assert(34-31 == 0b11)

    assert(42-35 == 0b111)
    assert(50-43 == 0b111)
    assert(58-51 == 0b111)
    assert(66-59 == 0b111)

    assert(82-67 == 0b1111)
    assert(98-83 == 0b1111)
    assert(114-99 == 0b1111)
    assert(130-115 == 0b1111)

    assert(162-131 == 0b11111)
    assert(194-163 == 0b11111)
    assert(226-195 == 0b11111)
    assert(255-227 <= 0b11100)

    codes.extend([0 for _ in range(0, 4)])
    # 0s
    codes.extend([i for i in range(257, 264)])
    # 1s
    codes.extend([264 for _ in range(11, 13)])
    codes.extend([265 for _ in range(13, 15)])
    codes.extend([266 for _ in range(15, 17)])
    codes.extend([267 for _ in range(17, 19)])
    # 2s
    codes.extend([268 for _ in range(19, 23)])
    codes.extend([269 for _ in range(23, 27)])
    codes.extend([270 for _ in range(27, 31)])
    codes.extend([271 for _ in range(31, 35)])
    # 3s
    codes.extend([272 for _ in range(35, 43)])
    codes.extend([273 for _ in range(43, 51)])
    codes.extend([274 for _ in range(51, 59)])
    codes.extend([275 for _ in range(59, 67)])
    # 4s
    codes.extend([276 for _ in range(67, 83)])
    codes.extend([277 for _ in range(83, 99)])
    codes.extend([278 for _ in range(99, 115)])
    codes.extend([279 for _ in range(115, 131)])

    codes.extend([280 for _ in range(131, 163)])
    codes.extend([281 for _ in range(163, 195)])
    codes.extend([282 for _ in range(195, 227)])
    codes.extend([283 for _ in range(227, 256)])

    assert(codes[4] == 257)
    assert(codes[5] == 258)
    assert(codes[6] == 259)
    assert(codes[7] == 260)
    assert(codes[8] == 261)
    assert(codes[9] == 262)
    assert(codes[10] == 263)

    for i in range(11, 256):

        if i >= 11 and i <= 12:
            assert(codes[i] == 264)
        if i >= 13 and i <= 14:
            assert(codes[i] == 265)
        if i >= 15 and i <= 16:
            assert(codes[i] == 266)
        if i >= 17 and i <= 18:
            assert(codes[i] == 267)

        if i >= 19 and i <= 22:
            assert(codes[i] == 268)
        if i >= 23 and i <= 26:
            assert(codes[i] == 269)
        if i >= 27 and i <= 30:
            assert(codes[i] == 270)
        if i >= 31 and i <= 34:
            assert(codes[i] == 271)

        if i >= 35 and i <= 42:
            assert(codes[i] == 272)
        if i >= 43 and i <= 50:
            assert(codes[i] == 273)
        if i >= 51 and i <= 58:
            assert(codes[i] == 274)
        if i >= 59 and i <= 66:
            assert(codes[i] == 275)

        if i >= 67 and i <= 82:
            assert(codes[i] == 276)
        if i >= 83 and i <= 98:
            assert(codes[i] == 277)
        if i >= 99 and i <= 114:
            assert(codes[i] == 278)
        if i >= 115 and i <= 130:
            assert(codes[i] == 279)

        if i >= 131 and i <= 162:
            assert(codes[i] == 280)
        if i >= 163 and i <= 194:
            assert(codes[i] == 281)
        if i >= 195 and i <= 226:
            assert(codes[i] == 282)
        if i >= 227 and i <= 255:
            assert(codes[i] == 283)

    print(codes)

    print(len(codes))


generate_match_len_codes()


# PREFIX INDEXES

#       Extra           Extra                 Extra

#  Code Bits IDXs   Code Bits  IDXs      Code Bits  IDXs

#  ---- ---- ----   ---- ----  ------     ---- ---- --------

#    0   1   0,1      10   6   124-187      20   11  4092-6139

#    1   1   2,3      11   6   188-251      21   11  6140-8187

#    2   2   4-7      12   7   252-379      22   12  8188-12283

#    3   2   8-11     13   7   380-507      23   12  12284-16379

#    4   3   12-19    14   8   508-763      24   13  16380-24571

#    5   3   20-27    15   8   764-1019     25   13  24572-32763

#    6   4   28-43    16   9   1020-1531    26   14  32764-49147

#    7   4   44-59    17   9   1532-2043    27   15  49148-81915

#    8   5   60-91    18  10   2044-3067    28   16  81916-147451

#    9   5   92-123   19  10   3068-4091    29   17  147452-278523

#                                           30   18  278524-540667

#                                           31   19  540668-1064955


# max prefixes = 1064956

def verify(b, n, m):
    if m-n != ((1 << b)-1):
        print(f"ERROR: {b, n, m} -> {m-n}: {((1<<b)-1)}")
    else:
        print("pass")


verify(1, 0, 1)
verify(1, 2, 3)
verify(2, 4, 7)
verify(2, 8, 11)
verify(3, 12, 19)
verify(3, 20, 27)
verify(4, 28, 43)
verify(4, 44, 59)
verify(5, 60, 91)
verify(5, 92, 123)
verify(6, 124, 187)
verify(6, 188, 251)
verify(7, 252, 379)
verify(7, 380, 507)
verify(8, 508, 763)
verify(8, 764, 1019)
verify(9, 1020, 1531)
verify(9, 1532, 2043)
verify(10, 2044, 3067)
verify(10, 3068, 4091)
verify(11, 4092, 6139)
verify(11, 6140, 8187)
verify(12, 8188, 12283)
verify(12, 12284, 16379)
verify(13, 16380, 24571)
verify(13, 24572, 32763)
verify(14, 32764, 49147)
verify(15, 49148, 81915)
verify(16, 81916, 147451)
verify(17, 147452, 278523)
verify(18, 278524, 540667)
verify(19, 540668, 1064955)
