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
    // doesn't check validity
    Piece doMove(Move move)
    {
        if (move.type == MoveType::Normal || move.type == MoveType::Promotion)
        {
            const Piece capturedPiece = m_pieces[ordinal(move.to)];
            const Piece fromPiece = m_pieces[ordinal(move.from)];
            const Piece toPiece = move.promotedPiece == Piece::none() ? fromPiece : move.promotedPiece;

            m_pieces[ordinal(move.to)] = toPiece;
            m_pieces[ordinal(move.from)] = Piece::none();

            m_pieceBB[ordinal(fromPiece)] ^= move.from;
            m_pieceBB[ordinal(toPiece)] ^= move.to;

            m_pieceBB[ordinal(capturedPiece)] ^= move.to;
            m_pieceBB[ordinal(Piece::none())] ^= move.from;

            return capturedPiece;
        }
        else if (move.type == MoveType::EnPassant)
        {
            const Piece movedPiece = m_pieces[ordinal(move.from)];
            const Piece capturedPiece(PieceType::Pawn, !movedPiece.color());
            const Square capturedPieceSq(move.to.file(), move.from.rank());

            // on ep move there are 3 squares involved
            m_pieces[ordinal(move.to)] = movedPiece;
            m_pieces[ordinal(move.from)] = Piece::none();
            m_pieces[ordinal(capturedPieceSq)] = Piece::none();

            m_pieceBB[ordinal(movedPiece)] ^= move.from;
            m_pieceBB[ordinal(movedPiece)] ^= move.to;

            m_pieceBB[ordinal(Piece::none())] ^= move.from;
            m_pieceBB[ordinal(Piece::none())] ^= move.to;

            m_pieceBB[ordinal(capturedPiece)] ^= capturedPieceSq;
            m_pieceBB[ordinal(Piece::none())] ^= capturedPieceSq;

            return capturedPiece;
        }
        else // if (move.type == MoveType::Castle)
        {
            // [color][isShort]
            constexpr Square rookDestinations[2][2] = { { F1, D1 }, { F8, D8 } };
            constexpr Square kingDestinations[2][2] = { { G1, C1 }, { G8, C8 } };

            const Square rookFromSq = move.to;
            const Square kingFromSq = move.from;

            const Piece rook = m_pieces[ordinal(rookFromSq)];
            const Piece king = m_pieces[ordinal(kingFromSq)];
            const Color color = king.color();
            const bool isShort = rookFromSq.file() == fileH;

            const Square rookToSq = rookDestinations[ordinal(color)][isShort];
            const Square kingToSq = kingDestinations[ordinal(color)][isShort];

            // 4 squares are involved
            m_pieces[ordinal(rookFromSq)] = Piece::none();
            m_pieces[ordinal(kingFromSq)] = Piece::none();
            m_pieces[ordinal(rookToSq)] = rook;
            m_pieces[ordinal(kingToSq)] = king;

            m_pieceBB[ordinal(rook)] ^= rookFromSq;
            m_pieceBB[ordinal(rook)] ^= rookToSq;

            m_pieceBB[ordinal(king)] ^= kingFromSq;
            m_pieceBB[ordinal(king)] ^= kingToSq;

            m_pieceBB[ordinal(Piece::none())] ^= rookFromSq;
            m_pieceBB[ordinal(Piece::none())] ^= rookToSq;

            m_pieceBB[ordinal(Piece::none())] ^= kingFromSq;
            m_pieceBB[ordinal(Piece::none())] ^= kingToSq;

            return Piece::none();
        }
    }

    void undoMove(Move move, Piece capturedPiece)
    {
        if (move.type == MoveType::Normal || move.type == MoveType::Promotion)
        {
            const Piece toPiece = m_pieces[ordinal(move.to)];
            const Piece fromPiece = move.promotedPiece == Piece::none() ? toPiece : Piece(PieceType::Pawn, toPiece.color());

            m_pieces[ordinal(move.from)] = fromPiece;
            m_pieces[ordinal(move.to)] = capturedPiece;

            m_pieceBB[ordinal(fromPiece)] ^= move.from;
            m_pieceBB[ordinal(toPiece)] ^= move.to;

            m_pieceBB[ordinal(capturedPiece)] ^= move.to;
            m_pieceBB[ordinal(Piece::none())] ^= move.from;
        }
        else if (move.type == MoveType::EnPassant)
        {
            const Piece movedPiece = m_pieces[ordinal(move.to)];
            const Piece capturedPiece(PieceType::Pawn, !movedPiece.color());
            const Square capturedPieceSq(move.to.file(), move.from.rank());

            m_pieces[ordinal(move.to)] = Piece::none();
            m_pieces[ordinal(move.from)] = movedPiece;
            m_pieces[ordinal(capturedPieceSq)] = capturedPiece;

            m_pieceBB[ordinal(movedPiece)] ^= move.from;
            m_pieceBB[ordinal(movedPiece)] ^= move.to;

            m_pieceBB[ordinal(Piece::none())] ^= move.from;
            m_pieceBB[ordinal(Piece::none())] ^= move.to;

            // on ep move there are 3 squares involved
            m_pieceBB[ordinal(capturedPiece)] ^= capturedPieceSq;
            m_pieceBB[ordinal(Piece::none())] ^= capturedPieceSq;
        }
        else // if (move.type == MoveType::Castle)
        {
            // [color][isShort]
            constexpr Square rookDestinations[2][2] = { { F1, D1 }, { F8, D8 } };
            constexpr Square kingDestinations[2][2] = { { G1, C1 }, { G8, C8 } };

            const Square rookFromSq = move.to;
            const Square kingFromSq = move.from;

            const Color color = move.to.rank() == rank1 ? Color::White : Color::Black;
            const bool isShort = rookFromSq.file() == fileH;

            const Square rookToSq = rookDestinations[ordinal(color)][isShort];
            const Square kingToSq = kingDestinations[ordinal(color)][isShort];

            const Piece rook = m_pieces[ordinal(rookToSq)];
            const Piece king = m_pieces[ordinal(kingToSq)];

            // 4 squares are involved
            m_pieces[ordinal(rookFromSq)] = rook;
            m_pieces[ordinal(kingFromSq)] = king;
            m_pieces[ordinal(rookToSq)] = Piece::none();
            m_pieces[ordinal(kingToSq)] = Piece::none();

            m_pieceBB[ordinal(rook)] ^= rookFromSq;
            m_pieceBB[ordinal(rook)] ^= rookToSq;

            m_pieceBB[ordinal(king)] ^= kingFromSq;
            m_pieceBB[ordinal(king)] ^= kingToSq;

            m_pieceBB[ordinal(Piece::none())] ^= rookFromSq;
            m_pieceBB[ordinal(Piece::none())] ^= rookToSq;

            m_pieceBB[ordinal(Piece::none())] ^= kingFromSq;
            m_pieceBB[ordinal(Piece::none())] ^= kingToSq;
        }
    }

    bool leavesKingInCheck(Move move, Color color) const
    {
        // checks whether by doing a move we uncover our king to a check
        // assumes that before the move the king was not in check
        // doesn't verify castlings as it is supposed to only cover undiscovered checks

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

    bool isPromotion(Square from, Square to) const
    {
        return m_pieces[ordinal(from)].type() == PieceType::Pawn && (to.rank() == rank1 || to.rank() == rank8);
    }

private:
    Piece m_pieces[cardinality<Square>()];
    Bitboard m_pieceBB[cardinality<Piece>()];

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

    Piece undoMove(Move move)
    {
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