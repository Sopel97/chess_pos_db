#pragma once

#include "Assert.h"
#include "Bitboard.h"
#include "EnumArray.h"
#include "Chess.h"

#include <iostream>
#include <iterator>
#include <string>

struct Board
{
private:
    static constexpr EnumArray2<Square, Color, bool> m_rookCastleDestinations = { { {{ D1, F1 }}, {{ D8, F8 }} } };
    static constexpr EnumArray2<Square, Color, bool> m_kingCastleDestinations = { { {{ C1, G1 }}, {{ C8, G8 }} } };

public:

    constexpr Board() noexcept
    {
        m_pieces.fill(Piece::none());
        m_pieceBB.fill(Bitboard::none());
        m_pieceBB[Piece::none()] = Bitboard::all();
    }

    // returns side to move
    [[nodiscard]] constexpr Color set(const char* fen)
    {
        ASSERT(fen != nullptr);

        File f = fileA;
        Rank r = rank8;
        auto current = fen;
        bool done = false;
        while (*current != '\0')
        {
            Piece piece = Piece::none();
            switch (*current)
            {
            case 'r':
                piece = Piece(PieceType::Rook, Color::Black);
                break;
            case 'n':
                piece = Piece(PieceType::Knight, Color::Black);
                break;
            case 'b':
                piece = Piece(PieceType::Bishop, Color::Black);
                break;
            case 'q':
                piece = Piece(PieceType::Queen, Color::Black);
                break;
            case 'k':
                piece = Piece(PieceType::King, Color::Black);
                break;
            case 'p':
                piece = Piece(PieceType::Pawn, Color::Black);
                break;

            case 'R':
                piece = Piece(PieceType::Rook, Color::White);
                break;
            case 'N':
                piece = Piece(PieceType::Knight, Color::White);
                break;
            case 'B':
                piece = Piece(PieceType::Bishop, Color::White);
                break;
            case 'Q':
                piece = Piece(PieceType::Queen, Color::White);
                break;
            case 'K':
                piece = Piece(PieceType::King, Color::White);
                break;
            case 'P':
                piece = Piece(PieceType::Pawn, Color::White);
                break;

            case ' ':
                done = true;
                break;

            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            {
                const int skip = (*current) - '0';
                f += skip;
                break;
            }

            case '/':
                f = fileA;
                --r;
                break;

            default:
                break;
            }

            if (done)
            {
                break;
            }

            if (piece != Piece::none())
            {
                place(piece, Square(f, r));
                ++f;
            }

            ++current;
        }

        return *(current + 1) == 'w' ? Color::White : Color::Black;
    }

    [[nodiscard]] constexpr friend bool operator==(const Board& lhs, const Board& rhs) noexcept
    {
        bool equal = true;
        for (Square sq = A1; sq <= H8; ++sq)
        {
            if (lhs.m_pieces[sq] != rhs.m_pieces[sq])
            {
                equal = false;
                break;
            }
        }

        ASSERT(bbsEqual(lhs, rhs) == equal);

        return equal;
    }

    constexpr void place(Piece piece, Square sq)
    {
        ASSERT(sq.isOk());

        m_pieceBB[m_pieces[sq]] ^= sq;
        m_pieces[sq] = piece;
        m_pieceBB[piece] |= sq;
    }

    constexpr void print(std::ostream& out) const
    {
        for (Rank r = rank8; r >= rank1; --r)
        {
            for (File f = fileA; f <= fileH; ++f)
            {
                out << toChar(m_pieces[Square(f, r)]);
            }
            out << '\n';
        }
    }

    // returns captured piece
    // doesn't check validity
    constexpr Piece doMove(Move move)
    {
        if (move.type == MoveType::Normal || move.type == MoveType::Promotion)
        {
            const Piece capturedPiece = m_pieces[move.to];
            const Piece fromPiece = m_pieces[move.from];
            const Piece toPiece = move.promotedPiece == Piece::none() ? fromPiece : move.promotedPiece;

            m_pieces[move.to] = toPiece;
            m_pieces[move.from] = Piece::none();

            m_pieceBB[fromPiece] ^= move.from;
            m_pieceBB[toPiece] ^= move.to;

            m_pieceBB[capturedPiece] ^= move.to;
            m_pieceBB[Piece::none()] ^= move.from;

            return capturedPiece;
        }
        else if (move.type == MoveType::EnPassant)
        {
            const Piece movedPiece = m_pieces[move.from];
            const Piece capturedPiece(PieceType::Pawn, !movedPiece.color());
            const Square capturedPieceSq(move.to.file(), move.from.rank());

            // on ep move there are 3 squares involved
            m_pieces[move.to] = movedPiece;
            m_pieces[move.from] = Piece::none();
            m_pieces[capturedPieceSq] = Piece::none();

            m_pieceBB[movedPiece] ^= move.from;
            m_pieceBB[movedPiece] ^= move.to;

            m_pieceBB[Piece::none()] ^= move.from;
            m_pieceBB[Piece::none()] ^= move.to;

            m_pieceBB[capturedPiece] ^= capturedPieceSq;
            m_pieceBB[Piece::none()] ^= capturedPieceSq;

            return capturedPiece;
        }
        else // if (move.type == MoveType::Castle)
        {
            const Square rookFromSq = move.to;
            const Square kingFromSq = move.from;

            const Piece rook = m_pieces[rookFromSq];
            const Piece king = m_pieces[kingFromSq];
            const Color color = king.color();
            const bool isShort = rookFromSq.file() == fileH;

            const Square rookToSq = m_rookCastleDestinations[color][isShort];
            const Square kingToSq = m_kingCastleDestinations[color][isShort];

            // 4 squares are involved
            m_pieces[rookFromSq] = Piece::none();
            m_pieces[kingFromSq] = Piece::none();
            m_pieces[rookToSq] = rook;
            m_pieces[kingToSq] = king;

            m_pieceBB[rook] ^= rookFromSq;
            m_pieceBB[rook] ^= rookToSq;

            m_pieceBB[king] ^= kingFromSq;
            m_pieceBB[king] ^= kingToSq;

            m_pieceBB[Piece::none()] ^= rookFromSq;
            m_pieceBB[Piece::none()] ^= rookToSq;

            m_pieceBB[Piece::none()] ^= kingFromSq;
            m_pieceBB[Piece::none()] ^= kingToSq;

            return Piece::none();
        }
    }

    constexpr void undoMove(Move move, Piece capturedPiece)
    {
        if (move.type == MoveType::Normal || move.type == MoveType::Promotion)
        {
            const Piece toPiece = m_pieces[move.to];
            const Piece fromPiece = move.promotedPiece == Piece::none() ? toPiece : Piece(PieceType::Pawn, toPiece.color());

            m_pieces[move.from] = fromPiece;
            m_pieces[move.to] = capturedPiece;

            m_pieceBB[fromPiece] ^= move.from;
            m_pieceBB[toPiece] ^= move.to;

            m_pieceBB[capturedPiece] ^= move.to;
            m_pieceBB[Piece::none()] ^= move.from;
        }
        else if (move.type == MoveType::EnPassant)
        {
            const Piece movedPiece = m_pieces[move.to];
            const Piece capturedPiece(PieceType::Pawn, !movedPiece.color());
            const Square capturedPieceSq(move.to.file(), move.from.rank());

            m_pieces[move.to] = Piece::none();
            m_pieces[move.from] = movedPiece;
            m_pieces[capturedPieceSq] = capturedPiece;

            m_pieceBB[movedPiece] ^= move.from;
            m_pieceBB[movedPiece] ^= move.to;

            m_pieceBB[Piece::none()] ^= move.from;
            m_pieceBB[Piece::none()] ^= move.to;

            // on ep move there are 3 squares involved
            m_pieceBB[capturedPiece] ^= capturedPieceSq;
            m_pieceBB[Piece::none()] ^= capturedPieceSq;
        }
        else // if (move.type == MoveType::Castle)
        {
            const Square rookFromSq = move.to;
            const Square kingFromSq = move.from;

            const Color color = move.to.rank() == rank1 ? Color::White : Color::Black;
            const bool isShort = rookFromSq.file() == fileH;

            const Square rookToSq = m_rookCastleDestinations[color][isShort];
            const Square kingToSq = m_kingCastleDestinations[color][isShort];

            const Piece rook = m_pieces[rookToSq];
            const Piece king = m_pieces[kingToSq];

            // 4 squares are involved
            m_pieces[rookFromSq] = rook;
            m_pieces[kingFromSq] = king;
            m_pieces[rookToSq] = Piece::none();
            m_pieces[kingToSq] = Piece::none();

            m_pieceBB[rook] ^= rookFromSq;
            m_pieceBB[rook] ^= rookToSq;

            m_pieceBB[king] ^= kingFromSq;
            m_pieceBB[king] ^= kingToSq;

            m_pieceBB[Piece::none()] ^= rookFromSq;
            m_pieceBB[Piece::none()] ^= rookToSq;

            m_pieceBB[Piece::none()] ^= kingFromSq;
            m_pieceBB[Piece::none()] ^= kingToSq;
        }
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool leavesKingInCheck(Move move, Color color) const
    {
        // checks whether by doing a move we uncover our king to a check
        // doesn't verify castlings as it is supposed to only cover undiscovered checks

        ASSERT(move.from.isOk() && move.to.isOk());

        const Square ksq = kingSquare(color);

        if (!bb::pseudoAttacks<PieceType::Queen>(ksq).isSet(move.from))
        {
            // if the square is not aligned with the king we don't have to check anything
            return false;
        }

        if (move.type == MoveType::Castle)
        {
            return false;
        }

        Bitboard occupied = (piecesBB() ^ move.from) | move.to;
        Bitboard captured = Bitboard::none();

        if (move.type == MoveType::EnPassant)
        {
            const Square capturedPieceSq(move.to.file(), move.from.rank());
            occupied ^= capturedPieceSq;
        }
        else if (m_pieces[move.to] != Piece::none())
        {
            // A capture happened.
            // We have to exclude the captured piece.
            captured |= move.to;
        }

        const Bitboard opponentQueens = piecesBB(Piece(PieceType::Queen, !color));

        const Bitboard opponentBishopLikePieces = (piecesBB(Piece(PieceType::Bishop, !color)) | opponentQueens) & ~captured;
        const Bitboard bishopAttacks = bb::attacks<PieceType::Bishop>(ksq, occupied);
        if ((bishopAttacks & opponentBishopLikePieces).any())
        {
            return true;
        }

        const Bitboard opponentRookLikePieces = (piecesBB(Piece(PieceType::Rook, !color)) | opponentQueens) & ~captured;
        const Bitboard rookAttacks = bb::attacks<PieceType::Rook>(ksq, occupied);
        return (rookAttacks & opponentRookLikePieces).any();
    }

    [[nodiscard]] constexpr Piece pieceAt(Square sq) const
    {
        ASSERT(sq.isOk());

        return m_pieces[sq];
    }

    [[nodiscard]] constexpr Bitboard piecesBB(Color c) const
    {
        return
            m_pieceBB[Piece(PieceType::Pawn, c)]
            | m_pieceBB[Piece(PieceType::Knight, c)]
            | m_pieceBB[Piece(PieceType::Bishop, c)]
            | m_pieceBB[Piece(PieceType::Rook, c)]
            | m_pieceBB[Piece(PieceType::Queen, c)]
            | m_pieceBB[Piece(PieceType::King, c)];
    }

    [[nodiscard]] INTRIN_CONSTEXPR Square kingSquare(Color c) const
    {
        return piecesBB(Piece(PieceType::King, c)).first();
    }

    [[nodiscard]] constexpr Bitboard piecesBB(Piece pc) const
    {
        return m_pieceBB[pc];
    }

    [[nodiscard]] constexpr Bitboard piecesBB() const
    {
        Bitboard bb{};

        // don't collect from null piece
        return piecesBB(Color::White) | piecesBB(Color::Black);

        return bb;
    }

    [[nodiscard]] constexpr bool isPromotion(Square from, Square to) const
    {
        ASSERT(from.isOk() && to.isOk());

        return m_pieces[from].type() == PieceType::Pawn && (to.rank() == rank1 || to.rank() == rank8);
    }

    const Piece* piecesRaw() const
    {
        return m_pieces.data();
    }

private:
    EnumArray<Piece, Square> m_pieces;
    EnumArray<Bitboard, Piece> m_pieceBB;

    // NOTE: currently we don't track it because it's not 
    // required to perform ep if we don't need to check validity
    // Square m_epSquare = Square::none(); 

    [[nodiscard]] static constexpr bool bbsEqual(const Board& lhs, const Board& rhs) noexcept
    {
        for (Piece pc : values<Piece>())
        {
            if (lhs.m_pieceBB[pc] != rhs.m_pieceBB[pc])
            {
                return false;
            }
        }

        return true;
    }
};

struct Position : public Board
{
    using BaseType = Board;

    constexpr Position() noexcept :
        Board(),
        m_sideToMove(Color::White)
    {
    }

    constexpr void set(const char* fen)
    {
        m_sideToMove = BaseType::set(fen);
    }

    [[nodiscard]] static constexpr Position fromFen(const char* fen)
    {
        Position pos{};
        pos.set(fen);
        return pos;
    }

    [[nodiscard]] static constexpr Position startPosition()
    {
        constexpr Position pos = fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");
        return pos;
    }

    constexpr Piece doMove(Move move)
    {
        ASSERT(move.from.isOk() && move.to.isOk());

        const Piece captured = BaseType::doMove(move);
        m_sideToMove = !m_sideToMove;
        return captured;
    }

    constexpr void undoMove(Move move, Piece capturedPiece)
    {
        BaseType::undoMove(move, capturedPiece);
        m_sideToMove = !m_sideToMove;
    }

    [[nodiscard]] constexpr Color sideToMove() const
    {
        return m_sideToMove;
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool leavesKingInCheck(Move move) const
    {
        return BaseType::leavesKingInCheck(move, m_sideToMove);
    }

    [[nodiscard]] constexpr bool friend operator==(const Position& lhs, const Position& rhs) noexcept
    {
        return lhs.m_sideToMove == rhs.m_sideToMove && static_cast<const Board&>(lhs) == static_cast<const Board&>(rhs);
    }

    // these are supposed to be used only for testing
    // that's why there's this assert in afterMove

    [[nodiscard]] constexpr Position beforeMove(Move move, Piece captured) const
    {
        Position cpy(*this);
        cpy.undoMove(move, captured);
        return cpy;
    }

    [[nodiscard]] constexpr Position afterMove(Move move) const
    {
        Position cpy(*this);
        auto pc = cpy.doMove(move);

        //ASSERT(cpy.beforeMove(move, pc) == *this);

        return cpy;
    }

private:
    Color m_sideToMove;
};

static_assert(Position::startPosition().afterMove(Move{ A2, A4 }) == Position::fromFen("rnbqkbnr/pppppppp/8/8/P7/8/1PPPPPPP/RNBQKBNR b KQkq -"));
static_assert(Position::startPosition().afterMove(Move{ E2, E3 }) == Position::fromFen("rnbqkbnr/pppppppp/8/8/8/4P3/PPPP1PPP/RNBQKBNR b KQkq -"));
static_assert(Position::startPosition().afterMove(Move{ G1, F3 }) == Position::fromFen("rnbqkbnr/pppppppp/8/8/8/5N2/PPPPPPPP/RNBQKB1R b KQkq -"));

static_assert(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -").afterMove(Move{ A7, A5 }) == Position::fromFen("rnbqkbnr/1ppppppp/8/p7/8/8/PPPPPPPP/RNBQKBNR w KQkq -"));
static_assert(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -").afterMove(Move{ E7, E6 }) == Position::fromFen("rnbqkbnr/pppp1ppp/4p3/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"));
static_assert(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -").afterMove(Move{ G8, F6 }) == Position::fromFen("rnbqkb1r/pppppppp/5n2/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"));

static_assert(Position::fromFen("k7/8/8/4pP2/8/8/8/K7 w - e6 0 2").afterMove(Move{ F5, E6, MoveType::EnPassant }) == Position::fromFen("k7/8/4P3/8/8/8/8/K7 b - -"));

static_assert(Position::fromFen("k4q2/4p3/3Q1Q2/8/8/8/8/5K2 w - - 0 1").afterMove(Move{ D6, E7 }) == Position::fromFen("k4q2/4Q3/5Q2/8/8/8/8/5K2 b - -"));
static_assert(Position::fromFen("k2q4/4p3/3Q1Q2/8/8/8/8/3K4 w - - 0 1").afterMove(Move{ F6, E7 }) == Position::fromFen("k2q4/4Q3/3Q4/8/8/8/8/3K4 b - -"));

static_assert(Position::fromFen("k7/8/3Q1Q2/4r3/3Q1Q2/8/8/3K4 w - - 0 1").afterMove(Move{ F6, E5 }) == Position::fromFen("k7/8/3Q4/4Q3/3Q1Q2/8/8/3K4 b - -"));

static_assert(Position::fromFen("k7/8/3Q1Q2/4r3/8/8/8/3K4 w - - 0 1").afterMove(Move{ F6, E5 }) == Position::fromFen("k7/8/3Q4/4Q3/8/8/8/3K4 b - -"));

static_assert(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1").afterMove(Move{ B4, D5 }) == Position::fromFen("k7/6N1/6N1/3N1NN1/8/8/8/3K4 b - -"));
static_assert(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1").afterMove(Move{ D1, C1 }) == Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/2K5 b - -"));
static_assert(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1").afterMove(Move{ F5, D4 }) == Position::fromFen("k7/6N1/6N1/3r2N1/1N1N4/8/8/3K4 b - -"));

static_assert(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1").afterMove(Move{ E5, H8 }) == Position::fromFen("7B/8/7B/8/6B1/k4B2/4B3/K7 b - -"));
static_assert(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1").afterMove(Move{ E5, G7 }) == Position::fromFen("8/6B1/7B/8/6B1/k4B2/4B3/K7 b - -"));
static_assert(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1").afterMove(Move{ H6, G7 }) == Position::fromFen("8/6B1/8/4B3/6B1/k4B2/4B3/K7 b - -"));
static_assert(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1").afterMove(Move{ F3, E4 }) == Position::fromFen("8/8/7B/4B3/4B1B1/k7/4B3/K7 b - -"));

static_assert(Position::fromFen("8/2B5/7B/2B5/k1B5/2B5/8/K7 w - - 0 1").afterMove(Move{ C7, E5 }) == Position::fromFen("8/8/7B/2B1B3/k1B5/2B5/8/K7 b - -"));

static_assert(Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ E1, H1, MoveType::Castle }) == Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R4RK1 b - - 1 1"));
static_assert(Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ E1, A1, MoveType::Castle }) == Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/2KR3R b - - 1 1"));

static_assert(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ D7, D8, MoveType::Promotion, whiteQueen }) == Position::fromFen("1k1Q4/6N1/5rN1/5NN1/1N6/8/8/R3K2R b KQ - 0 1"));
static_assert(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ D7, D8, MoveType::Promotion, whiteRook }) == Position::fromFen("1k1R4/6N1/5rN1/5NN1/1N6/8/8/R3K2R b KQ - 0 1"));
static_assert(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ D7, D8, MoveType::Promotion, whiteBishop }) == Position::fromFen("1k1B4/6N1/5rN1/5NN1/1N6/8/8/R3K2R b KQ - 0 1"));
static_assert(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ D7, D8, MoveType::Promotion, whiteKnight }) == Position::fromFen("1k1N4/6N1/5rN1/5NN1/1N6/8/8/R3K2R b KQ - 0 1"));

static_assert(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -").afterMove(Move{ E2, E1, MoveType::Promotion, blackQueen }) == Position::fromFen("k7/8/8/8/8/8/8/K3q3 w - - 0 2"));
static_assert(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -").afterMove(Move{ E2, E1, MoveType::Promotion, blackRook }) == Position::fromFen("k7/8/8/8/8/8/8/K3r3 w - - 0 2"));
static_assert(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -").afterMove(Move{ E2, E1, MoveType::Promotion, blackBishop }) == Position::fromFen("k7/8/8/8/8/8/8/K3b3 w - - 0 2"));
static_assert(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -").afterMove(Move{ E2, E1, MoveType::Promotion, blackKnight }) == Position::fromFen("k7/8/8/8/8/8/8/K3n3 w - - 0 2"));
