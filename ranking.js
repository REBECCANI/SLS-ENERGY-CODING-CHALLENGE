const calculateInteractionScore = (replyCount, retweetCount) => {
    return Math.log(1 + 2 * replyCount + retweetCount);
  };
  
  const calculateHashtagScore = (sameTagCount) => {
    if (sameTagCount > 10) {
      return 1 + Math.log(1 + sameTagCount - 10);
    } else {
      return 1;
    }
  };
  
  const calculateKeywordScore = (numberOfMatches) => {
    if (numberOfMatches > 0) {
      return 1 + Math.log(numberOfMatches + 1);
    } else {
      return 0;
    }
  };
  
  const calculateFinalScore = (interactionScore, hashtagScore, keywordScore) => {
    return parseFloat((interactionScore * hashtagScore * keywordScore).toFixed(5));
  };
  
  // Example function to get recommendation scores
  const getRecommendationScores = (userInteractions, userHashtags, userKeywords, query) => {
    const results = [];
  
    userInteractions.forEach(({ userId1, userId2, replyCount, retweetCount }) => {
      const interactionScore = calculateInteractionScore(replyCount, retweetCount);
      
      // Calculate hashtag score
      const commonHashtags = userHashtags[userId1].filter(tag => userHashtags[userId2].includes(tag));
      const sameTagCount = commonHashtags.length;
      const hashtagScore = calculateHashtagScore(sameTagCount);
      
      // Calculate keyword score
      let numberOfMatches = 0;
      userKeywords[userId1].forEach(keyword => {
        if (query.phrase && keyword.content.includes(query.phrase)) {
          numberOfMatches++;
        }
        if (query.hashtag && keyword.hashtags.includes(query.hashtag)) {
          numberOfMatches++;
        }
      });
      const keywordScore = calculateKeywordScore(numberOfMatches);
      
      // Calculate final score
      const finalScore = calculateFinalScore(interactionScore, hashtagScore, keywordScore);
      
      if (finalScore > 0) {
        results.push({ userId1, userId2, finalScore });
      }
    });
  
    // Sort results by final score in descending order
    results.sort((a, b) => b.finalScore - a.finalScore);
  
    return results;
  };
  