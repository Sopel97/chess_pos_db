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
    static constexpr EnumArray2<Square, Color, bool> m_rookCastleDestinations = { { F1, D1 }, { F8, D8 } };
    static constexpr EnumArray2<Square, Color, bool> m_kingCastleDestinations = { { G1, C1 }, { G8, C8 } };

public:

    constexpr Board() :
        m_pieces(Piece::none()),
        m_pieceBB(Bitboard::none())
    {
        m_pieceBB[Piece::none()] = Bitboard::all();
    }

    // returns side to move
    constexpr Color set(const char* fen)
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

    void place(Piece piece, Square sq)
    {
        ASSERT(sq.isOk());

        m_pieceBB[m_pieces[sq]] ^= sq;
        m_pieces[sq] = piece;
        m_pieceBB[piece] |= sq;
    }

    void print(std::ostream& out) const
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
    Piece doMove(Move move)
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

    void undoMove(Move move, Piece capturedPiece)
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

    bool leavesKingInCheck(Move move, Color color) const
    {
        // checks whether by doing a move we uncover our king to a check
        // assumes that before the move the king was not in check
        // doesn't verify castlings as it is supposed to only cover undiscovered checks

        ASSERT(move.from.isOk() && move.to.isOk());

        if (move.type == MoveType::Castle)
        {
            return false;
        }

        Bitboard occupied = (piecesBB() ^ move.from) | move.to;

        if (move.type == MoveType::EnPassant)
        {
            const Square capturedPieceSq(move.to.file(), move.from.rank());
            occupied ^= capturedPieceSq;
        }

        const Square ksq = kingSquare(color);

        const Bitboard bishopAttacks = bb::attacks<PieceType::Bishop>(ksq, occupied);
        const Bitboard rookAttacks = bb::attacks<PieceType::Rook>(ksq, occupied);

        const Bitboard opponentQueens = piecesBB(Piece(PieceType::Queen, !color));
        const Bitboard opponentBishopLikePieces = piecesBB(Piece(PieceType::Bishop, !color)) | opponentQueens;
        const Bitboard opponentRookLikePieces = piecesBB(Piece(PieceType::Rook, !color)) | opponentQueens;
        
        return (bishopAttacks & opponentBishopLikePieces).any() || (rookAttacks & opponentRookLikePieces).any();
    }

    Piece pieceAt(Square sq) const
    {
        ASSERT(sq.isOk());

        return m_pieces[sq];
    }

    Bitboard piecesBB(Color c) const
    {
        return
            m_pieceBB[Piece(PieceType::Pawn, c)]
            | m_pieceBB[Piece(PieceType::Knight, c)]
            | m_pieceBB[Piece(PieceType::Bishop, c)]
            | m_pieceBB[Piece(PieceType::Rook, c)]
            | m_pieceBB[Piece(PieceType::Queen, c)]
            | m_pieceBB[Piece(PieceType::King, c)];
    }

    Square kingSquare(Color c) const
    {
        return piecesBB(Piece(PieceType::King, c)).first();
    }

    Bitboard piecesBB(Piece pc) const
    {
        return m_pieceBB[pc];
    }

    Bitboard piecesBB() const
    {
        Bitboard bb{};

        // don't collect from null piece
        return piecesBB(Color::White) | piecesBB(Color::Black);

        return bb;
    }

    bool isPromotion(Square from, Square to) const
    {
        ASSERT(from.isOk() && to.isOk());

        return m_pieces[from].type() == PieceType::Pawn && (to.rank() == rank1 || to.rank() == rank8);
    }

private:
    EnumArray<Piece, Square> m_pieces;
    EnumArray<Bitboard, Piece> m_pieceBB;

    // NOTE: currently we don't track it because it's not 
    // required to perform ep if we don't need to check validity
    // Square m_epSquare = Square::none(); 
};

struct Position : public Board
{
    using BaseType = Board;

    constexpr Position() :
        Board(),
        m_sideToMove(Color::White)
    {
    }

    static constexpr Position fromFen(const char* fen)
    {
        Position pos{};
        pos.set(fen);
        return pos;
    }

    constexpr void set(const char* fen)
    {
        m_sideToMove = BaseType::set(fen);
    }

    Piece doMove(Move move)
    {
        ASSERT(move.from.isOk() && move.to.isOk());

        const Piece captured = BaseType::doMove(move);
        m_sideToMove = !m_sideToMove;
        return captured;
    }

    void undoMove(Move move, Piece capturedPiece)
    {
        BaseType::undoMove(move, capturedPiece);
        m_sideToMove = !m_sideToMove;
    }

    Color sideToMove() const
    {
        return m_sideToMove;
    }

    bool leavesKingInCheck(Move move) const
    {
        return BaseType::leavesKingInCheck(move, m_sideToMove);
    }

private:
    Color m_sideToMove;
};