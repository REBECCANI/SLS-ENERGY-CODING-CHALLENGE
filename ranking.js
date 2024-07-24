const fs = require('fs');

// Function to load popular hashtags from a file
const loadPopularHashtags = (filePath) => {
    return fs.readFileSync(filePath, 'utf-8').split('\n').map(tag => tag.trim().toLowerCase());
};

// Load popular hashtags
const popularHashtags = loadPopularHashtags('C:/Users/robot/Downloads/coding_challenge/challenge/popular_hashtags.txt');

// Function to calculate interaction score
const calculateInteractionScore = (replyCount, retweetCount) => {
    return Math.log(1 + 2 * replyCount + retweetCount);
};

// Function to calculate hashtag score with filtering of popular hashtags
const calculateHashtagScore = (sameTagCount) => {
    if (sameTagCount > 10) {
        return 1 + Math.log(1 + sameTagCount - 10);
    } else {
        return 1;
    }
};

// Function to calculate keyword score
const calculateKeywordScore = (numberOfMatches) => {
    if (numberOfMatches > 0) {
        return 1 + Math.log(numberOfMatches + 1);
    } else {
        return 0;
    }
};

// Function to calculate the final score
const calculateFinalScore = (interactionScore, hashtagScore, keywordScore) => {
    return parseFloat((interactionScore * hashtagScore * keywordScore).toFixed(5));
};

// Function to get recommendation scores
const getRecommendationScores = (userInteractions, userHashtags, userKeywords, query) => {
    const results = [];

    userInteractions.forEach(({ userId1, userId2, replyCount, retweetCount }) => {
        const interactionScore = calculateInteractionScore(replyCount, retweetCount);

        const hashtags1 = userHashtags[userId1] || [];
        const hashtags2 = userHashtags[userId2] || [];
        const commonHashtags = hashtags1.filter(tag => hashtags2.includes(tag) && !popularHashtags.includes(tag.toLowerCase()));
        const sameTagCount = commonHashtags.length;
        const hashtagScore = calculateHashtagScore(sameTagCount);

        let numberOfMatches = 0;
        const keywords = userKeywords[userId1] || [];
        keywords.forEach(keyword => {
            if (query.phrase && keyword.content.includes(query.phrase)) {
                numberOfMatches++;
            }
            if (query.hashtag && keyword.hashtags.includes(query.hashtag.toLowerCase())) {
                numberOfMatches++;
            }
        });
        const keywordScore = calculateKeywordScore(numberOfMatches);

        const finalScore = calculateFinalScore(interactionScore, hashtagScore, keywordScore);

        if (finalScore > 0) {
            results.push({ userId1, userId2, finalScore });
        }
    });

    results.sort((a, b) => b.finalScore - a.finalScore);

    return results;
};

// Export functions for external use
module.exports = {
    calculateInteractionScore,
    calculateHashtagScore,
    calculateKeywordScore,
    calculateFinalScore,
    getRecommendationScores
};
