#include "chess/Bcgn.h"
#include "chess/MoveGenerator.h"

#include <cstdlib>

void testBcgnWriter(std::string filename, bcgn::BcgnOptions options, int numGames, bcgn::BcgnWriter::FileOpenMode mode = bcgn::BcgnWriter::FileOpenMode::Truncate)
{
    srand(12345);

    bcgn::BcgnWriter writer(filename, options, mode);

    for (int i = 0; i < numGames; ++i)
    {
        auto pos = Position::startPosition();

        writer.beginGame();

        writer.setBlackElo(rand() % 2000 + 1000);
        writer.setWhiteElo(rand() % 2000 + 1000);
        writer.setDate(Date(2020, 4, 17));
        writer.setEco(Eco('E', 1));
        writer.setRound(i % 4000);
        writer.setWhitePlayer("whiteplayer");
        writer.setBlackPlayer("blackplayer");
        writer.setEvent("eventname");
        writer.setSite("sitesitesite");

        if (rand() % 10 == 0)
        {
            writer.setAdditionalTag("additionaltag1", "additionalvalue1");
            writer.setAdditionalTag("additionaltag2", "additionalvalue2");
        }

        if (rand() % 10 == 0)
        {
            writer.setCustomStartPos(pos);
        }

        int movecountInThisGame = 0;
        for (;;)
        {
            if (movecountInThisGame > 100) break;

            const auto moves = movegen::generateLegalMoves(pos);
            if (moves.empty()) break;

            ++movecountInThisGame;
            const auto move = moves[rand() % moves.size()];

            writer.addMove(pos, move);

            pos.doMove(move);
        }

        const auto randomResult = static_cast<GameResult>(rand() % 3);

        writer.setResult(randomResult);

        writer.endGame();
    }
}

void testBcgnWriter()
{
    constexpr int numGames = 256 * 32;

    {
        auto options = bcgn::BcgnOptions{};
        options.auxCompression = bcgn::BcgnAuxCompression::None;
        options.compressionLevel = bcgn::BcgnCompressionLevel::Level_0;
        options.version = bcgn::BcgnVersion::Version_0;
        testBcgnWriter("test_v0_c0_ac0.bcgn", options, numGames);
    }

    {
        auto options = bcgn::BcgnOptions{};
        options.auxCompression = bcgn::BcgnAuxCompression::None;
        options.compressionLevel = bcgn::BcgnCompressionLevel::Level_1;
        options.version = bcgn::BcgnVersion::Version_0;
        testBcgnWriter("test_v0_c1_ac0.bcgn", options, numGames);
    }

    {
        auto options = bcgn::BcgnOptions{};
        options.auxCompression = bcgn::BcgnAuxCompression::None;
        options.compressionLevel = bcgn::BcgnCompressionLevel::Level_0;
        options.version = bcgn::BcgnVersion::Version_0;
        testBcgnWriter("test_append.bcgn", options, numGames);
        testBcgnWriter("test_append.bcgn", options, numGames, bcgn::BcgnWriter::FileOpenMode::Append);
    }
}