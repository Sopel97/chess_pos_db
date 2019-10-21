#include "Position.h"

#include "detail/ParserBits.h"

#include "Bitboard.h"
#include "Chess.h"

#include "data_structure/EnumMap.h"

#include "util/Assert.h"

#include <iterator>
#include <optional>
#include <string>

#include "xxhash/xxhash_cpp.h"

[[nodiscard]] bool Board::createsDiscoveredAttackOnOwnKing(Move move, Color color) const
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

[[nodiscard]] bool Board::isSquareAttacked(Square sq, Color attackerColor, Bitboard occupied, Bitboard captured) const
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

[[nodiscard]] bool Board::isSquareAttacked(Square sq, Color attackerColor) const
{
    return isSquareAttacked(sq, attackerColor, piecesBB(), Bitboard::none());
}

[[nodiscard]] bool Board::isSquareAttackedAfterMove(Square sq, Move move, Color attackerColor) const
{
    // TODO: See whether this can be done better.
    Board cpy(*this);
    cpy.doMove(move);
    return cpy.isSquareAttacked(sq, attackerColor);
}

[[nodiscard]] bool Board::isKingAttackedAfterMove(Move move, Color kingColor) const
{
    // TODO: See whether this can be done better.
    Board cpy(*this);
    cpy.doMove(move);
    return cpy.isSquareAttacked(cpy.kingSquare(kingColor), !kingColor);
}

const Piece* Board::piecesRaw() const
{
    return m_pieces.data();
}

void Position::set(const char* fen)
{
    const char* s = BaseType::set(fen);

    s += 1;
    m_sideToMove = (*s == 'w') ? Color::White : Color::Black;

    s += 2;
    m_castlingRights = parser_bits::readCastlingRights(s);

    s += 1;
    m_epSquare = (*s == '-') ? Square::none() : parser_bits::parseSquare(s);

    nullifyEpSquareIfNotPossible();
}

// Returns false if the fen was not valid
// If the returned value was false the position
// is in unspecified state.
[[nodiscard]] bool Position::trySet(std::string_view fen)
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

    if (!BaseType::trySet(nextPart())) return false;

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

    nullifyEpSquareIfNotPossible();

    return true;
}

[[nodiscard]] Position Position::fromFen(const char* fen)
{
    Position pos{};
    pos.set(fen);
    return pos;
}

[[nodiscard]] std::optional<Position> Position::tryFromFen(std::string_view fen)
{
    Position pos{};
    if (pos.trySet(fen)) return pos;
    else return {};
}

[[nodiscard]] Position Position::startPosition()
{
    static const Position pos = fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");
    return pos;
}

[[nodiscard]] bool Position::createsDiscoveredAttackOnOwnKing(Move move) const
{
    return BaseType::createsDiscoveredAttackOnOwnKing(move, m_sideToMove);
}

[[nodiscard]] bool Position::createsAttackOnOwnKing(Move move) const
{
    return BaseType::isKingAttackedAfterMove(move, m_sideToMove);
}

[[nodiscard]] bool Position::isSquareAttackedAfterMove(Square sq, Move move, Color attackerColor) const
{
    return BaseType::isSquareAttackedAfterMove(sq, move, attackerColor);
}

[[nodiscard]] bool Position::isSquareAttacked(Square sq, Color attackerColor) const
{
    return BaseType::isSquareAttacked(sq, attackerColor);
}

ReverseMove Position::doMove(const Move& move)
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
            // Even though the move has not yet been made we can safely call
            // this function and get the right result because the position of the
            // pawn to be captured is not really relevant.
            if (isEpPossible(potentialEpSquare, !m_sideToMove))
            {
                m_epSquare = potentialEpSquare;
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

[[nodiscard]] bool Position::isLegal() const
{
    return piecesBB(Piece(PieceType::King, Color::White)).count() == 1
        && piecesBB(Piece(PieceType::King, Color::Black)).count() == 1
        && !isSquareAttacked(kingSquare(!m_sideToMove), m_sideToMove);
}

[[nodiscard]] bool Position::isCheck(Move move) const
{
    return BaseType::isSquareAttackedAfterMove(kingSquare(!m_sideToMove), move, m_sideToMove);
}

[[nodiscard]] Position Position::afterMove(Move move) const
{
    Position cpy(*this);
    auto pc = cpy.doMove(move);

    (void)pc;
    //ASSERT(cpy.beforeMove(move, pc) == *this); // this assert would result in infinite recursion

    return cpy;
}

[[nodiscard]] std::array<std::uint32_t, 4> Position::hash() const
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

[[nodiscard]] bool Position::isEpPossible(Square epSquare, Color sideToMove) const
{
    const Bitboard pawnsAttackingEpSquare =
        bb::pawnAttacks(Bitboard::square(epSquare), !sideToMove)
        & piecesBB(Piece(PieceType::Pawn, sideToMove));

    // only set m_epSquare when it matters, ie. when
    // the opposite side can actually capture
    for (Square sq : pawnsAttackingEpSquare)
    {
        if (!BaseType::createsDiscoveredAttackOnOwnKing(Move{ sq, epSquare, MoveType::EnPassant }, sideToMove))
        {
            return true;
        }
    }

    return false;
}

void Position::nullifyEpSquareIfNotPossible()
{
    if (m_epSquare != Square::none() && !isEpPossible(m_epSquare, m_sideToMove))
    {
        m_epSquare = Square::none();
    }
}
