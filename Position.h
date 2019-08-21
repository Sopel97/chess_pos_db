#pragma once

#include "Bitboard.h"
#include "Chess.h"

#include <iostream>
#include <iterator>
#include <string>

struct Board
{
    constexpr Board() :
        m_pieces{ Piece::none() },
        m_pieceBB{ Bitboard::none() }
    {
        m_pieceBB[ordinal(Piece::none())] = Bitboard::all();
    }

    // returns side to move
    constexpr Color set(const char* fen)
    {
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
        m_pieceBB[ordinal(m_pieces[ordinal(sq)])] ^= sq;
        m_pieces[ordinal(sq)] = piece;
        m_pieceBB[ordinal(piece)] |= sq;
    }

    void print(std::ostream& out) const
    {
        for (Rank r = rank8; r >= rank1; --r)
        {
            for (File f = fileA; f <= fileH; ++f)
            {
                out << toChar(m_pieces[ordinal(Square(f, r))]);
            }
            out << '\n';
        }
    }

    // returns captured piece
    Piece doMove(Move move)
    {
        const Piece capturedPiece = m_pieces[ordinal(move.to)];
        const Piece fromPiece = m_pieces[ordinal(move.from)];
        const Piece toPiece = move.promotedPiece == Piece::none() ? fromPiece : move.promotedPiece;

        m_pieces[ordinal(move.to)] = toPiece;
        m_pieces[ordinal(move.from)] = Piece::none();

        m_pieceBB[ordinal(fromPiece)] ^= move.from;
        m_pieceBB[ordinal(Piece::none())] ^= move.from;

        m_pieceBB[ordinal(toPiece)] ^= move.to;
        m_pieceBB[ordinal(capturedPiece)] ^= move.to;

        return capturedPiece;
    }

    void undoMove(Move move, Piece capturedPiece)
    {
        const Piece toPiece = m_pieces[ordinal(move.to)];
        const Piece fromPiece = move.promotedPiece == Piece::none() ? toPiece : Piece(PieceType::Pawn, toPiece.color());

        m_pieces[ordinal(move.from)] = fromPiece;
        m_pieces[ordinal(move.to)] = capturedPiece;

        m_pieceBB[ordinal(fromPiece)] ^= move.from;
        m_pieceBB[ordinal(Piece::none())] ^= move.from;

        m_pieceBB[ordinal(toPiece)] ^= move.to;
        m_pieceBB[ordinal(capturedPiece)] ^= move.to;
    }

    Piece pieceAt(Square sq) const
    {
        return m_pieces[ordinal(sq)];
    }

    Bitboard piecesBB(Color c) const
    {
        return
            m_pieceBB[ordinal(Piece(PieceType::Pawn, c))]
            | m_pieceBB[ordinal(Piece(PieceType::Knight, c))]
            | m_pieceBB[ordinal(Piece(PieceType::Bishop, c))]
            | m_pieceBB[ordinal(Piece(PieceType::Rook, c))]
            | m_pieceBB[ordinal(Piece(PieceType::Queen, c))]
            | m_pieceBB[ordinal(Piece(PieceType::King, c))];
    }

    Square kingSquare(Color c) const
    {
        return piecesBB(Piece(PieceType::King, c)).first();
    }

    Bitboard piecesBB(Piece pc) const
    {
        return m_pieceBB[ordinal(pc)];
    }

    Bitboard piecesBB() const
    {
        Bitboard bb{};

        // don't collect from null piece
        for (int i = 0; i < cardinality<Piece>() - 1; ++i)
        {
            bb |= m_pieceBB[i];
        }

        return bb;
    }

    bool isPromotion(Move move) const
    {
        return m_pieces[ordinal(move.from)].type() == PieceType::Pawn && (move.to.rank() == rank1 || move.to.rank() == rank8);
    }

private:
    Piece m_pieces[cardinality<Square>()];
    Bitboard m_pieceBB[cardinality<Piece>()];
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

    Piece undoMove(Move move)
    {
        const Piece captured = BaseType::doMove(move);
        m_sideToMove = opposite(m_sideToMove);
        return captured;
    }

    void undoMove(Move move, Piece capturedPiece)
    {
        BaseType::undoMove(move, capturedPiece);
        m_sideToMove = opposite(m_sideToMove);
    }

    Color sideToMove() const
    {
        return m_sideToMove;
    }

private:
    Color m_sideToMove;
};