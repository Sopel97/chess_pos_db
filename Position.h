#pragma once

#include "Assert.h"
#include "Bitboard.h"
#include "EnumMap.h"
#include "Chess.h"

#include <iostream>
#include <iterator>
#include <string>

#include "lib/xxhash/xxhash_cpp.h"

enum struct CastlingRights : std::uint8_t
{
    None = 0x0,
    WhiteKingSide = 0x1,
    WhiteQueenSide = 0x2,
    BlackKingSide = 0x4,
    BlackQueenSide = 0x8,
    White = WhiteKingSide | WhiteQueenSide,
    Black = BlackKingSide | BlackQueenSide,
    All = WhiteKingSide | WhiteQueenSide | BlackKingSide | BlackQueenSide
};

[[nodiscard]] constexpr CastlingRights operator|(CastlingRights lhs, CastlingRights rhs)
{
    return static_cast<CastlingRights>(static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs));
}

[[nodiscard]] constexpr CastlingRights operator&(CastlingRights lhs, CastlingRights rhs)
{
    return static_cast<CastlingRights>(static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs));
}

[[nodiscard]] constexpr CastlingRights operator~(CastlingRights lhs)
{
    return static_cast<CastlingRights>(~static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(CastlingRights::All));
}

constexpr CastlingRights& operator|=(CastlingRights& lhs, CastlingRights rhs)
{
    lhs = static_cast<CastlingRights>(static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs));
    return lhs;
}

constexpr CastlingRights& operator&=(CastlingRights& lhs, CastlingRights rhs)
{
    lhs = static_cast<CastlingRights>(static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs));
    return lhs;
}

constexpr CastlingRights moveToCastlingType(Move move)
{
    if (move.to == H1) return CastlingRights::WhiteKingSide;
    if (move.to == A1) return CastlingRights::WhiteQueenSide;
    if (move.to == H8) return CastlingRights::WhiteKingSide;
    if (move.to == A8) return CastlingRights::WhiteQueenSide;
    return CastlingRights::None;
}

// checks whether lhs contains rhs
[[nodiscard]] constexpr bool contains(CastlingRights lhs, CastlingRights rhs)
{
    return (lhs & rhs) == rhs;
}

template <>
struct EnumTraits<CastlingRights>
{
    using IdType = int;
    using EnumType = CastlingRights;

    static constexpr int cardinality = 4;
    static constexpr bool isNaturalIndex = false;

    static constexpr std::array<EnumType, cardinality> values{
        CastlingRights::WhiteKingSide,
        CastlingRights::WhiteQueenSide,
        CastlingRights::BlackKingSide,
        CastlingRights::BlackQueenSide
    };

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        return static_cast<EnumType>(id);
    }
};

namespace detail
{
    [[nodiscard]] constexpr bool isFile(char c)
    {
        return c >= 'a' && c <= 'h';
    }

    [[nodiscard]] constexpr bool isRank(char c)
    {
        return c >= '1' && c <= '8';
    }

    [[nodiscard]] constexpr Rank parseRank(char c)
    {
        ASSERT(isRank(c));

        return fromOrdinal<Rank>(c - '1');
    }

    [[nodiscard]] constexpr File parseFile(char c)
    {
        ASSERT(isFile(c));

        return fromOrdinal<File>(c - 'a');
    }

    [[nodiscard]] constexpr bool isSquare(const char* s)
    {
        return isRank(s[0]) && isFile(s[1]);
    }

    [[nodiscard]] constexpr Square parseSquare(const char* s)
    {
        const File file = parseFile(s[0]);
        const Rank rank = parseRank(s[1]);
        return Square(file, rank);
    }

    [[nodiscard]] constexpr std::optional<Square> tryParseSquare(std::string_view s)
    {
        if (s.size() != 2) return {};
        if (!isSquare(s.data())) return {};
        return parseSquare(s.data());
    }

    [[nodiscard]] constexpr std::optional<Square> tryParseEpSquare(std::string_view s)
    {
        if (s == "-"sv) return Square::none();
        return tryParseSquare(s);
    }

    [[nodiscard]] constexpr std::optional<CastlingRights> tryParseCastlingRights(std::string_view s)
    {
        if (s == "-"sv) return CastlingRights::None;

        CastlingRights rights = CastlingRights::None;

        for (auto& c : s)
        {
            CastlingRights toAdd = CastlingRights::None;
            switch (c)
            {
            case 'K':
                toAdd = CastlingRights::WhiteKingSide;
                break;
            case 'Q':
                toAdd = CastlingRights::WhiteQueenSide;
                break;
            case 'k':
                toAdd = CastlingRights::BlackKingSide;
                break;
            case 'q':
                toAdd = CastlingRights::BlackQueenSide;
                break;
            }

            // If there are duplicated castling rights specification we bail.
            // If there is an invalid character we bail.
            // (It always contains None)
            if (contains(rights, toAdd)) return {};
            else rights |= toAdd;
        }

        return rights;
    }

    [[nodiscard]] constexpr CastlingRights readCastlingRights(const char*& s)
    {
        CastlingRights rights = CastlingRights::None;

        while (*s != ' ')
        {
            switch (*s)
            {
            case 'K':
                rights |= CastlingRights::WhiteKingSide;
                break;
            case 'Q':
                rights |= CastlingRights::WhiteQueenSide;
                break;
            case 'k':
                rights |= CastlingRights::BlackKingSide;
                break;
            case 'q':
                rights |= CastlingRights::BlackQueenSide;
                break;
            }

            ++s;
        }

        return rights;
    }
}

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

    [[nodiscard]] INTRIN_CONSTEXPR bool createsDiscoveredAttackOnOwnKing(Move move, Color color) const
    {
        // checks whether by doing a move we uncover our king to a check
        // doesn't verify castlings as it is supposed to only cover undiscovered checks

        ASSERT(move.from.isOk() && move.to.isOk());
        ASSERT(move.type != MoveType::Castle);

        const Square ksq = kingSquare(color);

        ASSERT(ksq != move.from);

        if (move.type == MoveType::Castle)
        {
            return false;
        }

        Bitboard occupied = (piecesBB() ^ move.from) | move.to;
        Bitboard captured = Bitboard::none();
        Bitboard removed = Bitboard::square(move.from);

        if (move.type == MoveType::EnPassant)
        {
            const Square capturedPieceSq(move.to.file(), move.from.rank());
            occupied ^= capturedPieceSq;
            removed |= capturedPieceSq;
            // We don't update captured becuase it only affects pawns - we don't care.
        }
        else if (m_pieces[move.to] != Piece::none())
        {
            // A capture happened.
            // We have to exclude the captured piece.
            captured |= move.to;
        }

        const Bitboard allSliderPseudoAttacks = bb::pseudoAttacks<PieceType::Queen>(ksq);
        if (!(allSliderPseudoAttacks & removed).any())
        {
            // if the square is not aligned with the king we don't have to check anything
            return false;
        }

        const Bitboard bishops = piecesBB(Piece(PieceType::Bishop, !color)) & ~captured;
        const Bitboard rooks = piecesBB(Piece(PieceType::Rook, !color)) & ~captured;
        const Bitboard queens = piecesBB(Piece(PieceType::Queen, !color)) & ~captured;
        if (!(allSliderPseudoAttacks & (bishops | rooks | queens)).any())
        {
            return false;
        }

        return bb::isAttackedBySlider(
            ksq,
            bishops,
            rooks,
            queens,
            occupied
        );
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool isSquareAttacked(Square sq, Color attackerColor, Bitboard occupied, Bitboard captured) const
    {
        ASSERT(sq.isOk());

        const Bitboard bishops = piecesBB(Piece(PieceType::Bishop, attackerColor)) & ~captured;
        const Bitboard rooks = piecesBB(Piece(PieceType::Rook, attackerColor)) & ~captured;
        const Bitboard queens = piecesBB(Piece(PieceType::Queen, attackerColor)) & ~captured;
        if ((bb::pseudoAttacks<PieceType::Queen>(sq) & (bishops | rooks | queens)).any())
        {
            if (bb::isAttackedBySlider(
                sq,
                bishops,
                rooks,
                queens,
                occupied
            ))
            {
                return true;
            }
        }

        if (bb::pseudoAttacks<PieceType::King>(sq).isSet(kingSquare(attackerColor)))
        {
            return true;
        }

        if ((bb::pseudoAttacks<PieceType::Knight>(sq) & m_pieceBB[Piece(PieceType::Knight, attackerColor)] & ~captured).any())
        {
            return true;
        }

        // Check pawn attacks. Nothing else can attack the square at this point.
        const Bitboard pawns = m_pieceBB[Piece(PieceType::Pawn, attackerColor)] & ~captured;
        const Bitboard pawnAttacks = bb::pawnAttacks(pawns, attackerColor);

        return pawnAttacks.isSet(sq);
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool isSquareAttacked(Square sq, Color attackerColor) const
    {
        return isSquareAttacked(sq, attackerColor, piecesBB(), Bitboard::none());
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool isSquareAttackedAfterMove(Square sq, Move move, Color attackerColor) const
    {
        // TODO: See whether this can be done better.
        Board cpy(*this);
        cpy.doMove(move);
        return cpy.isSquareAttacked(sq, attackerColor);
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool isKingAttackedAfterMove(Move move, Color kingColor) const
    {
        // TODO: See whether this can be done better.
        Board cpy(*this);
        cpy.doMove(move);
        return cpy.isSquareAttacked(cpy.kingSquare(kingColor), !kingColor);
    }

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

    const Piece* piecesRaw() const
    {
        return m_pieces.data();
    }

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

struct ReverseMove
{
    Move move;
    Piece capturedPiece;
    Square oldEpSquare;
    CastlingRights oldCastlingRights;

    // We need a well defined case for the starting position.
    constexpr ReverseMove() :
        move(Move::null()),
        capturedPiece(Piece::none()),
        oldEpSquare(Square::none()),
        oldCastlingRights(CastlingRights::All)
    {
    }

    constexpr ReverseMove(const Move& move, Piece capturedPiece, Square oldEpSquare, CastlingRights oldCastlingRights) :
        move(move),
        capturedPiece(capturedPiece),
        oldEpSquare(oldEpSquare),
        oldCastlingRights(oldCastlingRights)
    {
    }
};

struct PackedReverseMove
{
    static constexpr std::uint32_t mask = 0x7FFFFFFu;
    static constexpr std::uint32_t squareMask = 0b111111u;
    static constexpr std::uint32_t pieceMask = 0b1111u;
    static constexpr std::uint32_t pieceTypeMask = 0b111u;
    static constexpr std::uint32_t castlingRightsMask = 0b1111;
    static constexpr std::uint32_t fileMask = 0b111;
    static constexpr std::size_t numBits = 27;

    constexpr PackedReverseMove(const ReverseMove& reverseMove) :
        m_packed(
            0u
            // The only move when square is none() is null move and
            // then both squares are none(). No other move is like that
            // so we don't lose any information by storing only
            // the 6 bits of each square.
            | ((ordinal(reverseMove.move.from) & squareMask) << 21)
            | ((ordinal(reverseMove.move.to) & squareMask) << 15)
            // Other masks are just for code clarity, they should
            // never change the values.
            | ((ordinal(reverseMove.capturedPiece) & pieceMask) << 11)
            | ((ordinal(reverseMove.oldCastlingRights) & castlingRightsMask) << 7)
            | ((ordinal(reverseMove.move.promotedPiece.type()) & pieceTypeMask) << 4)
            | (((reverseMove.oldEpSquare != Square::none()) & 1) << 3)
            // We probably could omit the squareMask here but for clarity it's left.
            | (ordinal(Square(ordinal(reverseMove.oldEpSquare) & squareMask).file()) & fileMask)
        )
    {
    }

    constexpr std::uint32_t packed() const
    {
        return m_packed;
    }

private:
    // Uses only 27 lowest bits.
    // Bit meaning from highest to lowest.
    // - 6 bits from
    // - 6 bits to
    // - 4 bits for the captured piece
    // - 4 bits for prev castling rights
    // - 3 bits promoted piece type
    // - 1 bit  to specify if the ep square was valid (false if none())
    // - 3 bits for prev ep square file
    std::uint32_t m_packed;
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
        m_castlingRights = detail::readCastlingRights(s);

        s += 1;
        m_epSquare = (*s == '-') ? Square::none() : detail::parseSquare(s);
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
            if (side == "w"sv) m_sideToMove = Color::White;
            else if (side == "b"sv) m_sideToMove = Color::Black;
            else return false;

            if (isSquareAttacked(kingSquare(!m_sideToMove), m_sideToMove)) return false;
        }

        {
            const auto castlingRights = nextPart();
            auto castlingRightsOpt = detail::tryParseCastlingRights(castlingRights);
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
            auto epSquareOpt = detail::tryParseEpSquare(epSquare);
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

    // TODO: if performance of setting special moves flags becomes 
    //       a problem then make a separate function that doesn't set it
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

    [[nodiscard]] INTRIN_CONSTEXPR bool createsDiscoveredAttackOnOwnKing(Move move) const
    {
        return BaseType::createsDiscoveredAttackOnOwnKing(move, m_sideToMove);
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool createsAttackOnOwnKing(Move move) const
    {
        return BaseType::isKingAttackedAfterMove(move, m_sideToMove);
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool isSquareAttackedAfterMove(Square sq, Move move, Color attackerColor) const
    {
        return BaseType::isSquareAttackedAfterMove(sq, move, attackerColor);
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool isSquareAttacked(Square sq, Color attackerColor) const
    {
        return BaseType::isSquareAttacked(sq, attackerColor);
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool isLegal() const
    {
        return piecesBB(Piece(PieceType::King, Color::White)).count() == 1
            && piecesBB(Piece(PieceType::King, Color::Black)).count() == 1
            && !isSquareAttacked(kingSquare(!m_sideToMove), m_sideToMove);
    }

    [[nodiscard]] INTRIN_CONSTEXPR bool isCheck(Move move) const
    {
        return BaseType::isSquareAttackedAfterMove(kingSquare(!m_sideToMove), move, m_sideToMove);
    }

    [[nodiscard]] Square epSquare() const
    {
        return m_epSquare;
    }

    [[nodiscard]] CastlingRights castlingRights() const
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

    [[nodiscard]] auto hash() const
    {
        constexpr std::size_t epSquareFileBits = 4;
        constexpr std::size_t castlingRightsBits = 4;

        std::array<std::uint32_t, 4> arrh;
        auto h = xxhash::XXH3_128bits(piecesRaw(), 64);
        std::memcpy(arrh.data(), &h, sizeof(std::uint32_t) * 4);
        arrh[0] ^= ordinal(m_sideToMove);

        arrh[0] <<= epSquareFileBits;
        // 0xF is certainly not a file number
        arrh[0] ^= m_epSquare == Square::none() ? 0xF : ordinal(m_epSquare);

        arrh[0] <<= castlingRightsBits;
        arrh[0] ^= ordinal(m_castlingRights);

        return arrh;
    }

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

static_assert(Position::fromFen("k7/6p1/5q2/5P2/8/8/5K2/8 b - - 0 1").piecesBB() == (Bitboard::square(F2) | F5 | F6 | G7 | A8));
static_assert(bb::isAttackedBySlider(F2, {}, {}, Bitboard::square(F6), (Bitboard::square(F2) | F6 | A8)));

static_assert(
    (
        bb::pawnAttacks(Bitboard::square(G6), Color::Black) &
        Position::fromFen("k7/8/5q2/5Pp1/8/8/5K2/8 w - - 0 2").piecesBB(Piece(PieceType::Pawn, Color::White))
    ) == Bitboard::square(F5));

static_assert(Position::fromFen("rnbqkbnr/ppp2ppp/8/3ppP2/8/4P3/PPPP2PP/RNBQKBNR b KQkq - 0 3").afterMove(Move{ G7, G5 }).isEpPossible());
static_assert(!Position::fromFen("rnb1kbnr/pp3ppp/5q2/2pppP2/8/2N1P3/PPPP1KPP/R1BQ1BNR b kq - 3 5").afterMove(Move{ G7, G5 }).isEpPossible());
static_assert(Position::fromFen("rnb1kbnr/pp3ppp/5q2/2pppP2/8/2N1P3/PPPP1KPP/R1BQ1BNR b kq - 3 5").afterMove(Move{ G7, G5 }).createsDiscoveredAttackOnOwnKing(Move{ F5, G6, MoveType::EnPassant }));

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



static_assert(Position::fromFen("k7/8/8/q2pP2K/8/8/8/8 w - d6 0 2").createsDiscoveredAttackOnOwnKing(Move{ E5, D6, MoveType::EnPassant }));
static_assert(!Position::fromFen("k7/8/q7/3pP2K/8/8/8/8 w - d6 0 1").createsDiscoveredAttackOnOwnKing(Move{ E5, D6, MoveType::EnPassant }));
static_assert(Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").createsDiscoveredAttackOnOwnKing(Move{ E5, D6, MoveType::EnPassant }));
static_assert(!Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").createsDiscoveredAttackOnOwnKing(Move{ E5, E6 }));


static_assert(Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").isSquareAttacked(C6, Color::Black));
static_assert(Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").isSquareAttacked(E4, Color::Black));
static_assert(Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").isSquareAttacked(D5, Color::Black));
static_assert(!Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").isSquareAttacked(H1, Color::Black));
static_assert(Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").isSquareAttacked(D6, Color::White));
static_assert(!Position::fromFen("k7/qb6/8/3pP3/8/5K2/8/8 w - -").isSquareAttacked(H7, Color::Black));

static_assert(!Position::fromFen("k7/qb6/8/3pP3/8/5K2/8/8 w - -").isSquareAttackedAfterMove(G7, Move{ A8, B8 }, Color::Black));
static_assert(Position::fromFen("k7/qb6/8/3pP3/8/5K2/8/8 w - -").isSquareAttackedAfterMove(G7, Move{ A7, G1 }, Color::Black));
static_assert(Position::fromFen("k7/1b6/8/q2pP3/8/5K2/8/8 w - d6").isSquareAttackedAfterMove(H5, Move{ E5, D6, MoveType::EnPassant }, Color::Black));
static_assert(Position::fromFen("k7/1b6/8/q2pP3/8/5K2/8/8 w - d6").isSquareAttackedAfterMove(E4, Move{ E5, D6, MoveType::EnPassant }, Color::Black));
static_assert(!Position::fromFen("k7/1b6/8/q2pP3/8/5K2/8/8 w - d6").isSquareAttackedAfterMove(H1, Move{ E5, D6, MoveType::EnPassant }, Color::Black));

static_assert(!Position::fromFen("rnb2k1r/pp1Pbppp/2p5/q7/2B5/8/PPPQNnPP/RNB1K2R w KQ - 0 1").createsAttackOnOwnKing(Move{ E1, H1, MoveType::Castle }));
