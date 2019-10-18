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
