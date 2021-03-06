#include "catch2/catch.hpp"

#include "chess/Bitboard.h"

TEST_CASE("Bitboard attacks", "[bitboard]")
{
    // random test cases generated with stockfish

    REQUIRE((bb::attacks(PieceType::Bishop, c7, 0x401f7ac78bc80f1c_bb) == 0x0a000a0000000000_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, f6, 0xf258d22d4db91392_bb) == 0x0050005088000000_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, b1, 0x67a7aabe10d172d6_bb) == 0x0000000010080500_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, a3, 0x05d07b7d1e8de386_bb) == 0x0000000002000200_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, b5, 0x583c502c832e0a3a_bb) == 0x0008050005080000_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, b3, 0x8f9fccba4388a61f_bb) == 0x0000000805000500_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, a2, 0x8b3a26b7aa4bcecb_bb) == 0x0000000000020002_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, b5, 0xdb696ab700feb090_bb) == 0x0008050005080000_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, h4, 0x6b5bd57a3c9113ef_bb) == 0x0000004000402010_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, h6, 0x3fc97b87bed94159_bb) == 0x0040004020000000_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, h2, 0x51efc5d2498d7506_bb) == 0x0000001020400040_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, c8, 0x2a327e8f39fc19a6_bb) == 0x000a100000000000_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, h2, 0x32c51436b7c00275_bb) == 0x0000000000400040_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, f6, 0xf7c35c861856282a_bb) == 0x0850005088000000_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, b7, 0x14a93ca1d9bcea61_bb) == 0x0500050000000000_bb));
    REQUIRE((bb::attacks(PieceType::Bishop, f4, 0x41dbe94941a43d12_bb) == 0x0000085000508800_bb));

    REQUIRE((bb::attacks(PieceType::Rook, b7, 0x957955653083196e_bb) == 0x020d020202020000_bb));
    REQUIRE((bb::attacks(PieceType::Rook, e8, 0x702751d1bb724213_bb) == 0x2f10100000000000_bb));
    REQUIRE((bb::attacks(PieceType::Rook, e3, 0x884bb2027e9ac7b0_bb) == 0x0000000010e81010_bb));
    REQUIRE((bb::attacks(PieceType::Rook, a3, 0x0ba88011cd101288_bb) == 0x00000000011e0101_bb));
    REQUIRE((bb::attacks(PieceType::Rook, a5, 0xb23cb1552b043b6e_bb) == 0x0000010601000000_bb));
    REQUIRE((bb::attacks(PieceType::Rook, f1, 0xe838ff59b1c9d964_bb) == 0x000000002020205c_bb));
    REQUIRE((bb::attacks(PieceType::Rook, b1, 0x26ebdcf08553011a_bb) == 0x000000000002020d_bb));
    REQUIRE((bb::attacks(PieceType::Rook, g8, 0x9ed34d63df99a685_bb) == 0xb040000000000000_bb));
    REQUIRE((bb::attacks(PieceType::Rook, d3, 0x5c7fc5fc683a1085_bb) == 0x0000000008160808_bb));
    REQUIRE((bb::attacks(PieceType::Rook, g4, 0x4c3fb0ceb4adb6b9_bb) == 0x00000040a0404040_bb));
    REQUIRE((bb::attacks(PieceType::Rook, c3, 0xec97f42c55bc9f40_bb) == 0x00000000040b0400_bb));
    REQUIRE((bb::attacks(PieceType::Rook, a6, 0xc149bd468ac1ac86_bb) == 0x0001060101010000_bb));
    REQUIRE((bb::attacks(PieceType::Rook, f6, 0xb906a73e05a92c74_bb) == 0x2020dc2000000000_bb));
    REQUIRE((bb::attacks(PieceType::Rook, e1, 0x7ca12fb5b05b5c4d_bb) == 0x0000000000001068_bb));
    REQUIRE((bb::attacks(PieceType::Rook, f1, 0xc27697252e02cb81_bb) == 0x00000000202020df_bb));
    REQUIRE((bb::attacks(PieceType::Rook, d4, 0x98d3daaa3b2e8562_bb) == 0x0000000816080000_bb));

    REQUIRE((bb::attacks(PieceType::Queen, f1, 0x45e0c63e93fc6383_bb) == 0x00000000000870de_bb));
    REQUIRE((bb::attacks(PieceType::Queen, h5, 0x38ddd8a535d2cbbd_bb) == 0x0000c060c0a01008_bb));
    REQUIRE((bb::attacks(PieceType::Queen, g2, 0x6f23d32e2a0fd7fa_bb) == 0x0000404850e0b0e0_bb));
    REQUIRE((bb::attacks(PieceType::Queen, h8, 0x360369eda9c0e07d_bb) == 0x60c0a08000000000_bb));
    REQUIRE((bb::attacks(PieceType::Queen, g7, 0x48bbb7a741e6ddd9_bb) == 0xe0a0e04040000000_bb));
    REQUIRE((bb::attacks(PieceType::Queen, f7, 0x5de152345f136375_bb) == 0x705f702000000000_bb));
    REQUIRE((bb::attacks(PieceType::Queen, d8, 0xdc22b9f9f9d7538d_bb) == 0x141c2a0100000000_bb));
    REQUIRE((bb::attacks(PieceType::Queen, h4, 0x05a6f16b79bbd6e9_bb) == 0x000080c040c02010_bb));
    REQUIRE((bb::attacks(PieceType::Queen, h3, 0xbc87a781b47462ce_bb) == 0x04081020c040c080_bb));
    REQUIRE((bb::attacks(PieceType::Queen, b5, 0x6c469ad3cba9b91a_bb) == 0x1008071d07080000_bb));
    REQUIRE((bb::attacks(PieceType::Queen, b1, 0xe8c41087c07c91fc_bb) == 0x00000002020a0705_bb));
    REQUIRE((bb::attacks(PieceType::Queen, h1, 0xfaec2f3c1e29110d_bb) == 0x0080808080a0c078_bb));
    REQUIRE((bb::attacks(PieceType::Queen, e6, 0x7cc1b5019ea1196d_bb) == 0x54382c3854800000_bb));
    REQUIRE((bb::attacks(PieceType::Queen, h6, 0x96b30966f70500d8_bb) == 0x20c078c080000000_bb));
    REQUIRE((bb::attacks(PieceType::Queen, b5, 0x74a51eba09dd373d_bb) == 0x0000070d070a0200_bb));
    REQUIRE((bb::attacks(PieceType::Queen, f7, 0xded20384ba4b0368_bb) == 0x705070a824020000_bb));
}

TEST_CASE("Bitboard between files", "[bitboard]") 
{
    REQUIRE((Bitboard::betweenFiles(fileA, fileG) == (Bitboard::all() ^ Bitboard::file(fileH))));
    REQUIRE((Bitboard::betweenFiles(fileB, fileG) == (Bitboard::all() ^ Bitboard::file(fileH) ^ Bitboard::file(fileA))));
    REQUIRE((Bitboard::betweenFiles(fileB, fileB) == (Bitboard::file(fileB))));
}