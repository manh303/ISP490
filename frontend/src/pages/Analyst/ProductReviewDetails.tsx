import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, MessageSquare, ThumbsUp, ThumbsDown, Star, Filter, Search } from 'lucide-react';
import { getProductReviewDetails, ProductReviewDetailsParams, ProductReviewDetailsResponse } from '../../services/DSSApi';

const ProductReviewDetails: React.FC = () => {
  const { productKey } = useParams<{ productKey: string }>();
  const navigate = useNavigate();
  const [reviewData, setReviewData] = useState<ProductReviewDetailsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState({
    sentiment_filter: 'all' as 'all' | 'positive' | 'negative' | 'neutral',
    sort_by: 'helpful_votes' as 'helpful_votes' | 'rating' | 'date',
    limit: 50
  });

  useEffect(() => {
    if (productKey) {
      fetchReviewDetails();
    }
  }, [productKey, filters]);

  const fetchReviewDetails = async () => {
    try {
      setLoading(true);
      const params: ProductReviewDetailsParams = {
        product_key: productKey!,
        ...filters
      };
      const data = await getProductReviewDetails(params);
      setReviewData(data);
    } catch (error) {
      console.error('Error fetching product review details:', error);
    } finally {
      setLoading(false);
    }
  };

  const getSentimentColor = (sentiment: string) => {
    switch (sentiment?.toLowerCase()) {
      case 'positive':
        return 'text-green-600 bg-green-100';
      case 'negative':
        return 'text-red-600 bg-red-100';
      case 'neutral':
        return 'text-yellow-600 bg-yellow-100';
      default:
        return 'text-gray-600 bg-gray-100';
    }
  };

  const getSentimentIcon = (sentiment: string) => {
    switch (sentiment?.toLowerCase()) {
      case 'positive':
        return <ThumbsUp className="w-4 h-4" />;
      case 'negative':
        return <ThumbsDown className="w-4 h-4" />;
      default:
        return <MessageSquare className="w-4 h-4" />;
    }
  };

  const renderStars = (rating: number) => {
    return (
      <div className="flex items-center">
        {[1, 2, 3, 4, 5].map((star) => (
          <Star
            key={star}
            className={`w-4 h-4 ${
              star <= rating ? 'text-yellow-400 fill-current' : 'text-gray-300'
            }`}
          />
        ))}
        <span className="ml-2 text-sm text-gray-600">{rating}/5</span>
      </div>
    );
  };

  const handleFilterChange = (key: string, value: any) => {
    setFilters(prev => ({ ...prev, [key]: value }));
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (!reviewData) {
    return (
      <div className="p-6">
        <div className="text-center py-12">
          <MessageSquare className="w-16 h-16 mx-auto mb-4 text-gray-400" />
          <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">
            No Review Data Found
          </h3>
          <p className="text-gray-600 dark:text-gray-300">
            Unable to load review details for this product.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <div className="mb-6">
        <button
          onClick={() => navigate(-1)}
          className="flex items-center text-blue-600 hover:text-blue-800 mb-4"
        >
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back
        </button>
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
              {reviewData.product_name}
            </h1>
            <p className="text-gray-600 dark:text-gray-300">
              Product Key: {reviewData.product_key}
            </p>
          </div>
          <div className="text-right">
            <p className="text-2xl font-bold text-gray-900 dark:text-white">
              {reviewData.total_reviews.toLocaleString()}
            </p>
            <p className="text-sm text-gray-600 dark:text-gray-300">Total Reviews</p>
          </div>
        </div>
      </div>

      {/* Sentiment Breakdown */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Positive</p>
              <p className="text-2xl font-bold text-green-600">
                {reviewData.sentiment_breakdown.positive}
              </p>
            </div>
            <ThumbsUp className="w-8 h-8 text-green-600" />
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Neutral</p>
              <p className="text-2xl font-bold text-yellow-600">
                {reviewData.sentiment_breakdown.neutral}
              </p>
            </div>
            <MessageSquare className="w-8 h-8 text-yellow-600" />
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Negative</p>
              <p className="text-2xl font-bold text-red-600">
                {reviewData.sentiment_breakdown.negative}
              </p>
            </div>
            <ThumbsDown className="w-8 h-8 text-red-600" />
          </div>
        </div>
      </div>

      {/* Filters */}
      <div className="bg-white dark:bg-gray-800 p-4 rounded-lg shadow border mb-6">
        <div className="flex items-center gap-4 flex-wrap">
          <Filter className="w-5 h-5 text-gray-500" />
          <span className="font-medium text-gray-900 dark:text-white">Filters</span>

          <select
            value={filters.sentiment_filter}
            onChange={(e) => handleFilterChange('sentiment_filter', e.target.value)}
            className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
          >
            <option value="all">All Sentiments</option>
            <option value="positive">Positive Only</option>
            <option value="negative">Negative Only</option>
            <option value="neutral">Neutral Only</option>
          </select>

          <select
            value={filters.sort_by}
            onChange={(e) => handleFilterChange('sort_by', e.target.value)}
            className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
          >
            <option value="helpful_votes">Sort by Helpful Votes</option>
            <option value="rating">Sort by Rating</option>
            <option value="date">Sort by Date</option>
          </select>

          <select
            value={filters.limit}
            onChange={(e) => handleFilterChange('limit', e.target.value)}
            className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
          >
            <option value={20}>20 reviews</option>
            <option value={50}>50 reviews</option>
            <option value={100}>100 reviews</option>
          </select>
        </div>
      </div>

      {/* Reviews List */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
        <div className="p-6 border-b">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
            <MessageSquare className="w-5 h-5 mr-2" />
            Customer Reviews ({reviewData.reviews.length})
          </h2>
        </div>
        <div className="divide-y divide-gray-200 dark:divide-gray-700">
          {reviewData.reviews.map((review) => (
            <div key={review?.review_id} className="p-6 hover:bg-gray-50 dark:hover:bg-gray-700">
              <div className="flex items-start justify-between mb-4">
                <div className="flex-1">
                  <div className="flex items-center gap-3 mb-2">
                    {renderStars(review?.rating)}
                    <span className={`inline-flex items-center gap-1 px-2 py-1 text-xs rounded-full ${getSentimentColor(review?.sentiment_label)}`}>
                      {getSentimentIcon(review?.sentiment_label)}
                      {review?.sentiment_label}
                    </span>
                    <span className="text-sm text-gray-500">
                      Sentiment Score: {review?.sentiment_score?.toFixed(2)}
                    </span>
                  </div>

                  {review?.review_title && (
                    <h3 className="font-medium text-gray-900 dark:text-white mb-2">
                      {review?.review_title}
                    </h3>
                  )}

                  <p className="text-gray-700 dark:text-gray-300 mb-3">
                    {review?.review_body || 'No review text available'}
                  </p>

                  <div className="flex items-center gap-4 text-sm text-gray-500">
                    <span>By {review?.reviewer_name}</span>
                    <span>{new Date(review?.review_date)?.toLocaleDateString()}</span>
                    <span className="flex items-center gap-1">
                      <ThumbsUp className="w-3 h-3" />
                      {review?.helpful_votes} helpful votes
                    </span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>

        {reviewData.reviews.length === 0 && (
          <div className="p-12 text-center">
            <MessageSquare className="w-16 h-16 mx-auto mb-4 text-gray-400" />
            <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">
              No Reviews Found
            </h3>
            <p className="text-gray-600 dark:text-gray-300">
              No reviews match the current filter criteria.
            </p>
          </div>
        )}
      </div>
    </div>
  );
};

export default ProductReviewDetails;