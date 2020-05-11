#include "chess/ReverseMoveGenerator.h"
#include "chess/Eran.h"

void assertMoveIncluded(std::string_view fen, std::string_view eran)
{
    const Position pos = Position::tryFromFen(fen).value();
    const ReverseMove rm = eran::eranToReverseMove(pos, eran);
    bool ok = false;
    movegen::forEachPseudoLegalReverseMove(pos, movegen::PieceSet::standardPieceSet(), [&rm, &ok](const ReverseMove& genrm) {
        if (rm == genrm) ok = true;
        });
    TEST_ASSERT(ok);
}

void assertMoveNotIncluded(std::string_view fen, std::string_view eran)
{
    const Position pos = Position::tryFromFen(fen).value();
    const ReverseMove rm = eran::eranToReverseMove(pos, eran);
    bool ok = true;
    movegen::forEachPseudoLegalReverseMove(pos, movegen::PieceSet::standardPieceSet(), [&rm, &ok](const ReverseMove& genrm) {
        if (rm == genrm) ok = false;
        });
    TEST_ASSERT(ok);
}

void assertMoveNotIncluded(std::string_view fen, ReverseMove rm)
{
    const Position pos = Position::tryFromFen(fen).value();
    bool ok = true;
    movegen::forEachPseudoLegalReverseMove(pos, movegen::PieceSet::standardPieceSet(), [&rm, &ok](const ReverseMove& genrm) {
        if (rm == genrm) ok = false;
        });
    TEST_ASSERT(ok);
}

void assertMoveCount(std::string_view fen, int c)
{
    const Position pos = Position::tryFromFen(fen).value();
    movegen::forEachPseudoLegalReverseMove(pos, movegen::PieceSet::standardPieceSet(), [&c](const ReverseMove& genrm) {
        --c;
        });
    TEST_ASSERT(c == 0);
}

void test()
{
    assertMoveCount("rnbqkbnr/pp1ppppp/8/8/2pP4/8/PPP1PPPP/RNBQKBNR b KQkq d3 0 1", 1);
    assertMoveCount("rnbqkbnr/p2p1p1p/8/1p2pPpP/2pP4/8/PPP1P1P1/RNBQKBNR b KQkq d3 0 1", 3);

    assertMoveIncluded("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1", "Na6-b8 KQkq -");
    assertMoveIncluded("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w - - 0 1", "Na6-b8 - -");
    assertMoveIncluded("rnbqkbnr/p2p1p1p/8/1p2pPpP/2pP4/8/PPP1P1P1/RNBQKBNR b KQkq d3 0 1", "d2-d4 KQkq e6");
    assertMoveIncluded("1nbqkbnr/3p1p1p/1p6/1p2pPpP/2pP4/1P6/1PP1P1P1/rNBQKBNR w Kk - 0 1", "Ra2xRa1 KQk -");
    assertMoveIncluded("1nbqkbnr/3p1p1p/1p6/1p2pPpP/2pP4/1P6/1PP1P1P1/rNBQKBNR w Kk - 0 1", "Ra8xRa1 KQkq -");
    assertMoveIncluded("1nbqkbnr/3p1p1p/1p6/1p2pPpP/2pP4/1P6/1PP1PKP1/rNBQ1BNR b k - 0 1", "Ke1-f2 Kk -");
    assertMoveIncluded("1nbqkbnr/4pp1p/1p6/1p2pPpP/2pP4/1P6/1PP1PKP1/rNBQ1BNR b k - 0 1", "Ke1-f2 Kk g6");
    assertMoveIncluded("1nbqkb1r/4pp1p/1p3nQ1/1p2pPpP/2pP4/1P6/1PP1PKP1/rNB2BNR b k - 0 1", "Qg8-g6 k g6");
    assertMoveIncluded("1nbqkb1r/4pp1p/1p3nQ1/1p2pPpP/2pP4/1P6/1PP1PKP1/rNB2BNR b k - 0 1", "Qh6-g6 k g6");
    assertMoveIncluded("2bqkb1r/4pp1p/1p3nQ1/1p2pPpP/2pP4/1P6/1PP1PKP1/rNB2BNR b k - 0 1", "g4xNh5 k -");
    assertMoveIncluded("2bqkb1r/1n2pp1p/5nQ1/2P1pPpP/8/1P6/1PP1PKP1/rNB2BNR b k - 0 1", "d4xc5 k -");
    assertMoveIncluded("2bqkb1r/1n2pp1p/5nQ1/2PPp1pP/8/1P6/1PP1PKP1/rNB2BNR b k - 0 1", "d4xc5 k c6");
    assertMoveIncluded("rnbqkbnr/ppp1pppp/3P4/8/8/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1", "e5xd6 KQkq d6");
    assertMoveIncluded("rNbqkbnr/pp2pppp/3P4/8/8/8/PPP2PPP/R1BQKBNR b KQkq - 0 1", "c7xNb8N KQkq -");

    assertMoveNotIncluded("1nbqkbnr/3p1p1p/1p6/1p2pPpP/2pP4/1P6/1PP1PKP1/rNBQ1BNR b k - 0 1", "Ke1-f2 KQk -");
    assertMoveNotIncluded("1nbqkbnr/4pp1p/1p6/1p2pPpP/2pP4/1P6/1PP1PKP1/rNBQ1BNR b k - 0 1", "Ke1-f2 Kk e6");
    assertMoveNotIncluded("1nbqkb1r/4pp1p/1p3nQ1/1p2pPpP/2pP4/1P6/1PP1PKP1/rNB2BNR b k - 0 1", "Qh6xg6 k g6");
    assertMoveNotIncluded("1nbqkb1r/4pp1p/1p3nQ1/1p2pPpP/2pP4/1P6/1PP1PKP1/rNB2BNR b k - 0 1", "Qg7-g6 k g6");
    assertMoveNotIncluded("2bqkb1r/4pp1p/1p3nQ1/1p2pPpP/2pP4/1P6/1PP1PKP1/rNB2BNR b k - 0 1", "Qh6xNg6 k g6");
    assertMoveNotIncluded("2bqkb1r/1n2pp1p/1p3nQ1/1p2pPpP/2pP4/1P6/1PP1PKP1/rNB2BNR b k - 0 1", "g4xNh5 k -");
    assertMoveNotIncluded("2bqkb1r/1n2pp1p/5nQ1/2P1pPpP/8/1P6/1PP1PKP1/rNB2BNR b k - 0 1", "d4xc5 k c6");
    assertMoveNotIncluded("2bqkb1r/1nP2p1p/5nQ1/2pP2pP/8/1P6/1PP2KP1/rNB2BNR b k - 0 1", "d6xc7 k c6");

    // These illegal reverse move cannot be specified in ERAN. It's interpreted as normal pawn capture.
    // assertMoveNotIncluded("rnbqkbnr/ppp1pppp/3P4/8/8/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1", "e5xd6 KQkq -");
    // assertMoveNotIncluded("rnbqkbnr/ppp1p1pp/3P4/5pP1/8/8/PPPP2PP/RNBQKBNR b KQkq - 0 1", "e5xd6 KQkq f6");
    assertMoveNotIncluded("rnbqkbnr/ppp1pppp/3P4/8/8/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1", ReverseMove{ Move::enPassant(e5, d6), Piece::none(), Square::none(), CastlingRights::All });
    assertMoveNotIncluded("rnbqkbnr/ppp1pppp/3P4/8/8/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1", ReverseMove{ Move::enPassant(e5, d6), Piece::none(), f6, CastlingRights::All });
}