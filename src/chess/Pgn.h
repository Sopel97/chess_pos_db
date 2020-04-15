#pragma once

#include "Date.h"
#include "Eco.h"
#include "GameClassification.h"
#include "Position.h"

#include "util/Assert.h"

#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace pgn
{
    using namespace std::literals;

    struct TagView
    {
        std::string_view key;
        std::string_view value;
    };

    struct UnparsedGamePositions
    {
        struct UnparsedPositionsIterator
        {
            struct Sentinel {};

            using value_type = Position;
            using difference_type = std::ptrdiff_t;
            using reference = const Position &;
            using iterator_category = std::input_iterator_tag;
            using pointer = const Position*;

            UnparsedPositionsIterator(std::string_view moveSection) noexcept;

            const UnparsedPositionsIterator& operator++();

            bool friend operator==(const UnparsedPositionsIterator& lhs, Sentinel rhs) noexcept;

            bool friend operator!=(const UnparsedPositionsIterator& lhs, Sentinel rhs) noexcept;

            [[nodiscard]] const Position& operator*() const;

            [[nodiscard]] const Position* operator->() const;

        private:
            Position m_position;
            std::string_view m_moveSection;
        };

        using iterator = UnparsedPositionsIterator;
        using const_iterator = UnparsedPositionsIterator;

        UnparsedGamePositions(std::string_view moveSection) noexcept;

        [[nodiscard]] UnparsedPositionsIterator begin();

        [[nodiscard]] UnparsedPositionsIterator::Sentinel end() const;

    private:
        std::string_view m_moveSection;
    };

    struct UnparsedGameMoves
    {
        struct UnparsedMovesIterator
        {
            struct Sentinel {};

            using value_type = Position;
            using difference_type = std::ptrdiff_t;
            using reference = const Position &;
            using iterator_category = std::input_iterator_tag;
            using pointer = const Position*;

            UnparsedMovesIterator(std::string_view moveSection) noexcept;

            const UnparsedMovesIterator& operator++();

            bool friend operator==(const UnparsedMovesIterator& lhs, Sentinel rhs) noexcept;

            bool friend operator!=(const UnparsedMovesIterator& lhs, Sentinel rhs) noexcept;

            [[nodiscard]] const std::string_view& operator*() const;

            [[nodiscard]] const std::string_view* operator->() const;

        private:
            std::string_view m_san;
            std::string_view m_moveSection;
        };

        using iterator = UnparsedMovesIterator;
        using const_iterator = UnparsedMovesIterator;

        UnparsedGameMoves(std::string_view moveSection) noexcept;

        [[nodiscard]] UnparsedMovesIterator begin();

        [[nodiscard]] UnparsedMovesIterator::Sentinel end() const;

    private:
        std::string_view m_moveSection;
    };

    struct UnparsedGameTags
    {
        struct UnparsedTagsIterator
        {
            struct Sentinel {};

            using value_type = TagView;
            using difference_type = std::ptrdiff_t;
            using reference = const TagView &;
            using iterator_category = std::input_iterator_tag;
            using pointer = const TagView*;

            UnparsedTagsIterator(std::string_view tagSection) noexcept;

            const UnparsedTagsIterator& operator++();

            bool friend operator==(const UnparsedTagsIterator& lhs, Sentinel rhs) noexcept;

            bool friend operator!=(const UnparsedTagsIterator& lhs, Sentinel rhs) noexcept;

            [[nodiscard]] const TagView& operator*() const;

            [[nodiscard]] const TagView* operator->() const;

        private:
            std::string_view m_tagSection;
            TagView m_tag;
        };

        using iterator = UnparsedTagsIterator;
        using const_iterator = UnparsedTagsIterator;

        UnparsedGameTags(std::string_view tagSection) noexcept;

        [[nodiscard]] UnparsedTagsIterator begin();

        [[nodiscard]] UnparsedTagsIterator::Sentinel end() const;

    private:
        std::string_view m_tagSection;
    };

    struct UnparsedGame
    {
        explicit UnparsedGame();

        UnparsedGame(std::string_view tagSection, std::string_view moveSection) noexcept;

        void getResultDateEcoEventWhiteBlack(
            std::optional<GameResult>& result,
            Date& date,
            Eco& eco,
            std::string_view& event,
            std::string_view& white,
            std::string_view& black
        ) const;

        void getResultDateEcoEventWhiteBlackPlyCount(
            std::optional<GameResult>& result,
            Date& date,
            Eco& eco,
            std::string_view& event,
            std::string_view& white,
            std::string_view& black,
            std::uint16_t& plyCount
        ) const;

        [[nodiscard]] std::int64_t eloDiff() const;

        [[nodiscard]] std::optional<GameResult> result() const;

        [[nodiscard]] Date date() const;

        [[nodiscard]] Eco eco() const;

        [[nodiscard]] std::uint16_t plyCount() const;

        [[nodiscard]] std::uint16_t plyCount(std::uint16_t def) const;

        [[nodiscard]] std::string_view tag(std::string_view tag) const;

        [[nodiscard]] std::string_view tagSection() const;

        [[nodiscard]] std::string_view moveSection() const;

        [[nodiscard]] UnparsedGamePositions positions() const;

        [[nodiscard]] UnparsedGameMoves moves() const;

        [[nodiscard]] UnparsedGameTags tags() const;

    private:
        std::string_view m_tagSection;
        std::string_view m_moveSection;
    };

    // is supposed to work as a game iterator
    // stores the current game
    struct LazyPgnFileReader
    {
    private:
        // currently bufferSize must be bigger than the maximum number of bytes taken by a single game
        // TODO: resize buffer when didn't process anything
        static constexpr std::size_t m_minBufferSize = 128ull * 1024ull;

    public:
        struct LazyPgnFileReaderIterator
        {
            struct Sentinel {};

            using value_type = UnparsedGame;
            using difference_type = std::ptrdiff_t;
            using reference = const UnparsedGame &;
            using iterator_category = std::input_iterator_tag;
            using pointer = const UnparsedGame*;

            LazyPgnFileReaderIterator(const std::filesystem::path& path, std::size_t bufferSize);

            const LazyPgnFileReaderIterator& operator++();

            bool friend operator==(const LazyPgnFileReaderIterator& lhs, Sentinel rhs) noexcept;

            bool friend operator!=(const LazyPgnFileReaderIterator& lhs, Sentinel rhs) noexcept;

            [[nodiscard]] const UnparsedGame& operator*() const;

            [[nodiscard]] const UnparsedGame* operator->() const;

        private:
            std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
            std::size_t m_bufferSize;
            std::vector<char> m_buffer;
            std::vector<char> m_auxBuffer;
            std::future<std::size_t> m_future;
            std::size_t m_auxBufferLeft;
            std::string_view m_bufferView; // what is currently being processed
            UnparsedGame m_game;

            [[nodiscard]] bool isEnd() const;

            void moveToNextGame();

            NOINLINE void refillBuffer();
        };

        using iterator = LazyPgnFileReaderIterator;
        using const_iterator = LazyPgnFileReaderIterator;

        // We keep the file opened. That way we weakly enforce that a created iterator
        // (that reopens the file to have it's own cursor)
        // is valid after a successful call to isOpen()
        LazyPgnFileReader(const std::filesystem::path& path, std::size_t bufferSize = m_minBufferSize);

        [[nodiscard]] bool isOpen() const;

        [[nodiscard]] LazyPgnFileReaderIterator begin();

        [[nodiscard]] LazyPgnFileReaderIterator::Sentinel end() const;

    private:
        std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
        std::filesystem::path m_path;
        std::size_t m_bufferSize;
    };
}
