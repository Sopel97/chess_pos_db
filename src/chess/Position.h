#pragma once

#include "chess/detail/ParserBits.h"

#include "util/Assert.h"
#include "Bitboard.h"
#include "data_structure/EnumMap.h"
#include "Chess.h"

#include <iostream>
#include <iterator>
#include <string>
#include <optional>

#include "xxhash/xxhash_cpp.h"

struct Board
{
private:
    static constexpr EnumMap2<Color, CastleType, Square> m_rookCastleDestinations = { { {{ F1, D1 }}, {{ F8, D8 }} } };
    static constexpr EnumMap2<Color, CastleType, Square> m_kingCastleDestinations = { { {{ G1, C1 }}, {{ G8, C8 }} } };

public:

    constexpr Board() noexcept
    {
        m_pieces.fill(Piece::none());
        m_pieceBB.fill(Bitboard::none());
        m_pieceBB[Piece::none()] = Bitboard::all();
        m_piecesByColorBB.fill(Bitboard::none());
    }

    [[nodiscard]] constexpr bool isValid() const
    {
        if (piecesBB(whiteKing).count() != 1) return false;
        if (piecesBB(blackKing).count() != 1) return false;
        if (((piecesBB(whitePawn) | piecesBB(blackPawn)) & (bb::rank(rank1) | bb::rank(rank8))).any()) return false;
        return true;
    }

    [[nodiscard]] constexpr bool trySet(std::string_view boardState)
    {
        File f = fileA;
        Rank r = rank8;
        bool lastWasSkip = false;
        for (auto c : boardState)
        {
            Piece piece = Piece::none();
            switch (c)
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

            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            {
                if (lastWasSkip) return false;
                lastWasSkip = true;

                const int skip = c - '0';
                f += skip;
                if (f > fileH + 1) return false;
                break;
            }

            case '/':
                lastWasSkip = false;
                if (f != fileH + 1) return false;
                f = fileA;
                --r;
                break;

            default:
                return false;
            }

            if (piece != Piece::none())
            {
                lastWasSkip = false;

                const Square sq(f, r);
                if (!sq.isOk()) return false;

                place(piece, sq);
                ++f;
            }
        }

        if (f != fileH + 1) return false;
        if (r != rank1) return false;

        return isValid();
    }

    // returns side to move
    [[nodiscard]] constexpr const char* set(const char* fen)
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

        return current;
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

        auto oldPiece = m_pieces[sq];
        m_pieceBB[oldPiece] ^= sq;
        if (oldPiece != Piece::none())
        {
            m_piecesByColorBB[oldPiece.color()] ^= sq;
        }
        m_pieces[sq] = piece;
        m_pieceBB[piece] |= sq;
        m_piecesByColorBB[piece.color()] |= sq;
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
        if (move.type == MoveType::Normal)
        {
            const Piece capturedPiece = m_pieces[move.to];
            const Piece piece = m_pieces[move.from];

            m_pieces[move.to] = piece;
            m_pieces[move.from] = Piece::none();

            m_pieceBB[piece] ^= move.from;
            m_pieceBB[piece] ^= move.to;

            m_pieceBB[capturedPiece] ^= move.to;
            m_pieceBB[Piece::none()] ^= move.from;

            m_piecesByColorBB[piece.color()] ^= move.to;
            m_piecesByColorBB[piece.color()] ^= move.from;
            if (capturedPiece != Piece::none())
            {
                m_piecesByColorBB[capturedPiece.color()] ^= move.to;
            }

            return capturedPiece;
        }
        else if (move.type == MoveType::Promotion)
        {
            // We split it even though it's similar just because
            // the normal case is much more common.
            const Piece capturedPiece = m_pieces[move.to];
            const Piece fromPiece = m_pieces[move.from];
            const Piece toPiece = move.promotedPiece;

            m_pieces[move.to] = toPiece;
            m_pieces[move.from] = Piece::none();

            m_pieceBB[fromPiece] ^= move.from;
            m_pieceBB[toPiece] ^= move.to;

            m_pieceBB[capturedPiece] ^= move.to;
            m_pieceBB[Piece::none()] ^= move.from;

            m_piecesByColorBB[fromPiece.color()] ^= move.to;
            m_piecesByColorBB[fromPiece.color()] ^= move.from;
            if (capturedPiece != Piece::none())
            {
                m_piecesByColorBB[capturedPiece.color()] ^= move.to;
            }

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

            m_piecesByColorBB[movedPiece.color()] ^= move.to;
            m_piecesByColorBB[movedPiece.color()] ^= move.from;
            m_piecesByColorBB[capturedPiece.color()] ^= capturedPieceSq;

            return capturedPiece;
        }
        else // if (move.type == MoveType::Castle)
        {
            const Square rookFromSq = move.to;
            const Square kingFromSq = move.from;

            const Piece rook = m_pieces[rookFromSq];
            const Piece king = m_pieces[kingFromSq];
            const Color color = king.color();
            const CastleType castleType = (rookFromSq.file() == fileH) ? CastleType::Short : CastleType::Long;

            const Square rookToSq = m_rookCastleDestinations[color][castleType];
            const Square kingToSq = m_kingCastleDestinations[color][castleType];

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

            m_piecesByColorBB[color] ^= rookFromSq;
            m_piecesByColorBB[color] ^= rookToSq;
            m_piecesByColorBB[color] ^= kingFromSq;
            m_piecesByColorBB[color] ^= kingToSq;

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

            m_piecesByColorBB[fromPiece.color()] ^= move.to;
            m_piecesByColorBB[fromPiece.color()] ^= move.from;
            if (capturedPiece != Piece::none())
            {
                m_piecesByColorBB[capturedPiece.color()] ^= move.to;
            }
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

            m_piecesByColorBB[movedPiece.color()] ^= move.to;
            m_piecesByColorBB[movedPiece.color()] ^= move.from;
            m_piecesByColorBB[capturedPiece.color()] ^= capturedPieceSq;
        }
        else // if (move.type == MoveType::Castle)
        {
            const Square rookFromSq = move.to;
            const Square kingFromSq = move.from;

            const Color color = move.to.rank() == rank1 ? Color::White : Color::Black;
            const CastleType castleType = (rookFromSq.file() == fileH) ? CastleType::Short : CastleType::Long;

            const Square rookToSq = m_rookCastleDestinations[color][castleType];
            const Square kingToSq = m_kingCastleDestinations[color][castleType];

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

            m_piecesByColorBB[color] ^= rookFromSq;
            m_piecesByColorBB[color] ^= rookToSq;
            m_piecesByColorBB[color] ^= kingFromSq;
            m_piecesByColorBB[color] ^= kingToSq;
        }
    }

    [[nodiscard]] bool createsDiscoveredAttackOnOwnKing(Move move, Color color) const;

    [[nodiscard]] bool isSquareAttacked(Square sq, Color attackerColor, Bitboard occupied, Bitboard captured) const;

    [[nodiscard]] bool isSquareAttacked(Square sq, Color attackerColor) const;

    [[nodiscard]] bool isSquareAttackedAfterMove(Square sq, Move move, Color attackerColor) const;

    [[nodiscard]] bool isKingAttackedAfterMove(Move move, Color kingColor) const;

    [[nodiscard]] constexpr Piece pieceAt(Square sq) const
    {
        ASSERT(sq.isOk());

        return m_pieces[sq];
    }

    [[nodiscard]] constexpr Bitboard piecesBB(Color c) const
    {
        return m_piecesByColorBB[c];
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

    const Piece* piecesRaw() const;

private:
    EnumMap<Square, Piece> m_pieces;
    EnumMap<Piece, Bitboard> m_pieceBB;
    EnumMap<Color, Bitboard> m_piecesByColorBB;

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
        m_sideToMove(Color::White),
        m_epSquare(Square::none()),
        m_castlingRights(CastlingRights::All)
    {
    }

    constexpr void set(const char* fen)
    {
        const char* s = BaseType::set(fen);

        s += 1;
        m_sideToMove = (*s == 'w') ? Color::White : Color::Black;

        s += 2;
        m_castlingRights = parser_bits::readCastlingRights(s);

        s += 1;
        m_epSquare = (*s == '-') ? Square::none() : parser_bits::parseSquare(s);
    }

    // Returns false if the fen was not valid
    // If the returned value was false the position
    // is in unspecified state.
    constexpr bool trySet(std::string_view fen)
    {
        // Lazily splits by ' '. Returns empty string views if at the end.
        auto nextPart = [fen, start = std::size_t{ 0 }]() mutable {
            std::size_t end = fen.find(' ', start);
            if (end == std::string::npos)
            {
                std::string_view substr = fen.substr(start);
                start = fen.size();
                return substr;
            }
            else
            {
                std::string_view substr = fen.substr(start, end - start);
                start = end + 1; // to skip whitespace
                return substr;
            }
        };

        if(!BaseType::trySet(nextPart())) return false;

        {
            const auto side = nextPart();
            if (side == std::string_view("w")) m_sideToMove = Color::White;
            else if (side == std::string_view("b")) m_sideToMove = Color::Black;
            else return false;

            if (isSquareAttacked(kingSquare(!m_sideToMove), m_sideToMove)) return false;
        }

        {
            const auto castlingRights = nextPart();
            auto castlingRightsOpt = parser_bits::tryParseCastlingRights(castlingRights);
            if (!castlingRightsOpt.has_value())
            {
                return false;
            }
            else
            {
                m_castlingRights = *castlingRightsOpt;
            }
        }

        {
            const auto epSquare = nextPart();
            auto epSquareOpt = parser_bits::tryParseEpSquare(epSquare);
            if (!epSquareOpt.has_value())
            {
                return false;
            }
            else
            {
                m_epSquare = *epSquareOpt;
            }
        }

        return true;
    }

    [[nodiscard]] static constexpr Position fromFen(const char* fen)
    {
        Position pos{};
        pos.set(fen);
        return pos;
    }

    [[nodiscard]] static constexpr std::optional<Position> tryFromFen(std::string_view fen)
    {
        Position pos{};
        if (pos.trySet(fen)) return pos;
        else return {};
    }

    [[nodiscard]] static constexpr Position startPosition()
    {
        constexpr Position pos = fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");
        return pos;
    }

    constexpr ReverseMove doMove(const Move& move)
    {
        ASSERT(move.from.isOk() && move.to.isOk());

        const PieceType movedPiece = pieceAt(move.from).type();
        const Square oldEpSquare = m_epSquare;
        const CastlingRights oldCastlingRights = m_castlingRights;

        m_epSquare = Square::none();
        switch (movedPiece)
        {
        case PieceType::Pawn:
        {
            const int d = move.to.rank() - move.from.rank();
            if (d == -2 || d == 2)
            {
                const Square potentialEpSquare = Square(move.from.file(), move.from.rank() + d / 2);
                const Bitboard pawnsAttackingEpSquare =
                    bb::pawnAttacks(Bitboard::square(potentialEpSquare), m_sideToMove)
                    & piecesBB(Piece(PieceType::Pawn, !m_sideToMove));

                // only set m_epSquare when it matters, ie. when
                // the opposite side can actually capture
                for (Square sq : pawnsAttackingEpSquare)
                {
                    if (!BaseType::createsDiscoveredAttackOnOwnKing(Move{ sq, potentialEpSquare, MoveType::EnPassant }, !m_sideToMove))
                    {
                        m_epSquare = potentialEpSquare;
                        break;
                    }
                }
            }
            break;
        }
        case PieceType::King:
        {
            if (move.from == E1) m_castlingRights &= ~CastlingRights::White;
            else if (move.from == E8) m_castlingRights &= ~CastlingRights::Black;
            break;
        }
        case PieceType::Rook:
        {
            if (move.from == H1) m_castlingRights &= ~CastlingRights::WhiteKingSide;
            else if (move.from == A1) m_castlingRights &= ~CastlingRights::WhiteQueenSide;
            else if (move.from == H8) m_castlingRights &= ~CastlingRights::BlackKingSide;
            else if (move.from == A8) m_castlingRights &= ~CastlingRights::BlackQueenSide;
            break;
        }
        default:
            break;
        }

        const Piece captured = BaseType::doMove(move);
        m_sideToMove = !m_sideToMove;
        return { move, captured, oldEpSquare, oldCastlingRights };
    }

    constexpr void undoMove(const ReverseMove& reverseMove)
    {
        const Move& move = reverseMove.move;
        BaseType::undoMove(move, reverseMove.capturedPiece);

        m_epSquare = reverseMove.oldEpSquare;
        m_castlingRights = reverseMove.oldCastlingRights;

        m_sideToMove = !m_sideToMove;
    }

    [[nodiscard]] constexpr Color sideToMove() const
    {
        return m_sideToMove;
    }

    [[nodiscard]] bool createsDiscoveredAttackOnOwnKing(Move move) const;

    [[nodiscard]] bool createsAttackOnOwnKing(Move move) const;

    [[nodiscard]] bool isSquareAttackedAfterMove(Square sq, Move move, Color attackerColor) const;

    [[nodiscard]] bool isSquareAttacked(Square sq, Color attackerColor) const;

    [[nodiscard]] bool isLegal() const;

    [[nodiscard]] bool isCheck(Move move) const;

    [[nodiscard]] constexpr Square epSquare() const
    {
        return m_epSquare;
    }

    [[nodiscard]] constexpr CastlingRights castlingRights() const
    {
        return m_castlingRights;
    }

    [[nodiscard]] constexpr bool friend operator==(const Position& lhs, const Position& rhs) noexcept
    {
        // TODO: ep and castling rights equality
        return lhs.m_sideToMove == rhs.m_sideToMove && static_cast<const Board&>(lhs) == static_cast<const Board&>(rhs);
    }

    // these are supposed to be used only for testing
    // that's why there's this assert in afterMove

    [[nodiscard]] constexpr Position beforeMove(const ReverseMove& reverseMove) const
    {
        Position cpy(*this);
        cpy.undoMove(reverseMove);
        return cpy;
    }

    [[nodiscard]] constexpr Position afterMove(Move move) const
    {
        Position cpy(*this);
        auto pc = cpy.doMove(move);

        (void)pc;
        //ASSERT(cpy.beforeMove(move, pc) == *this); // this assert would result in infinite recursion

        return cpy;
    }

    [[nodiscard]] std::array<std::uint32_t, 4> hash() const;

    [[nodiscard]] constexpr bool isEpPossible() const
    {
        return m_epSquare != Square::none();
    }

private:
    Color m_sideToMove;
    Square m_epSquare;
    CastlingRights m_castlingRights;
};

static_assert(sizeof(Position) == 192);
