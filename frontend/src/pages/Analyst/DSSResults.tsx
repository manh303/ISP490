import React from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { ArrowLeft, TrendingUp, Users, MessageSquare, Lightbulb, Target, AlertTriangle } from 'lucide-react';
import { ResponsiveContainer, PieChart, Pie, Cell, Tooltip } from 'recharts';

interface DSSResultsProps {}

const DSSResults: React.FC<DSSResultsProps> = () => {
  const { modelId } = useParams<{ modelId: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const { inputData, dssResults } = location.state || {};

  const models = {
    price_prediction: {
      name: 'Dự đoán Giá',
      icon: <TrendingUp className="w-6 h-6" />,
      color: '#3B82F6'
    },
    product_recommendation: {
      name: 'Gợi ý Sản phẩm',
      icon: <Users className="w-6 h-6" />,
      color: '#10B981'
    },
    review_sentiment: {
      name: 'Phân tích Cảm xúc Đánh giá',
      icon: <MessageSquare className="w-6 h-6" />,
      color: '#F59E0B'
    }
  };

  const currentModel = models[modelId as keyof typeof models];

  if (!currentModel) {
    return <div>Model not found</div>;
  }

  // Mock data for different models (fallback if API not available)
  const getMockData = () => {
    switch (modelId) {
      case 'price_prediction':
        return {
          mlResults: {
            chartData: [
              { date: '2025-11-01', price: 1200000, predicted: 1180000, lower: 1150000, upper: 1210000 },
              { date: '2025-11-02', price: 1190000, predicted: 1175000, lower: 1145000, upper: 1205000 },
              { date: '2025-11-03', price: 1210000, predicted: 1190000, lower: 1160000, upper: 1220000 },
              { date: '2025-11-04', price: 1180000, predicted: 1165000, lower: 1135000, upper: 1195000 },
              { date: '2025-11-05', price: 1220000, predicted: 1200000, lower: 1170000, upper: 1230000 },
            ],
            currentPrice: 1200000,
            predictedPrice: 1185000,
            confidenceInterval: { lower: 1155000, upper: 1215000 },
            accuracy: 92.5
          },
          aiSummary: {
            insights: [
              "Giá sản phẩm đang có xu hướng giảm nhẹ trong tuần qua",
              "Mức giá dự đoán 1,185,000 VND nằm trong khoảng tin cậy 1,155,000 - 1,215,000 VND",
              "Độ chính xác của mô hình là 92.5%, cho thấy dự đoán khá đáng tin cậy"
            ],
            anomalies: [
              "Giá thực tế ngày 03/11 cao hơn dự kiến 2%, có thể do chương trình khuyến mãi đặc biệt"
            ],
            risks: [
              "Rủi ro cạnh tranh giá từ đối thủ trên cùng platform",
              "Thị trường có thể biến động nếu có sự kiện đặc biệt trong tháng 12"
            ]
          },
          aiActions: [
            {
              type: 'pricing',
              title: 'Giảm giá 5% cho sản phẩm này',
              description: 'Dựa trên dự đoán giá, đề xuất giảm 5% để tăng tính cạnh tranh',
              impact: 'Cao',
              effort: 'Thấp',
              priority: 'Cao'
            },
            {
              type: 'marketing',
              title: 'Tăng quảng cáo trên Tiki',
              description: 'Tăng ngân sách quảng cáo 20% trong tuần tới để tận dụng xu hướng giá',
              impact: 'Trung bình',
              effort: 'Trung bình',
              priority: 'Trung bình'
            },
            {
              type: 'inventory',
              title: 'Điều chỉnh tồn kho',
              description: 'Giảm nhập hàng 10% trong tháng tới để tránh tồn kho',
              impact: 'Thấp',
              effort: 'Thấp',
              priority: 'Thấp'
            }
          ]
        };

      case 'product_recommendation':
        return {
          mlResults: {
            recommendations: [
              { rank: 1, product: 'iPhone 15 Pro', similarity: 0.95, price: 25000000, rating: 4.8 },
              { rank: 2, product: 'Samsung Galaxy S24', similarity: 0.89, price: 22000000, rating: 4.7 },
              { rank: 3, product: 'Google Pixel 8', similarity: 0.82, price: 18000000, rating: 4.5 },
              { rank: 4, product: 'OnePlus 12', similarity: 0.78, price: 16000000, rating: 4.4 },
              { rank: 5, product: 'Xiaomi 14', similarity: 0.75, price: 14000000, rating: 4.3 }
            ],
            totalProducts: 1247,
            avgSimilarity: 0.84
          },
          aiSummary: {
            insights: [
              "Khách hàng có sở thích mạnh với smartphone cao cấp (iPhone, Samsung)",
              "Độ tương đồng cao nhất là 95% với iPhone 15 Pro",
              "Trung bình độ tương đồng của top 5 sản phẩm là 84%"
            ],
            anomalies: [
              "Khách hàng này chưa mua smartphone trong 6 tháng qua",
              "Có xu hướng tìm kiếm sản phẩm giá cao hơn mức trung bình"
            ],
            risks: [
              "Rủi ro khách hàng hủy đơn nếu giá thay đổi",
              "Cạnh tranh từ flash sale có thể làm giảm tỷ lệ chuyển đổi"
            ]
          },
          aiActions: [
            {
              type: 'promotion',
              title: 'Gửi voucher 500k cho iPhone 15 Pro',
              description: 'Tạo ưu đãi đặc biệt cho sản phẩm được recommend cao nhất',
              impact: 'Cao',
              effort: 'Thấp',
              priority: 'Cao'
            },
            {
              type: 'email',
              title: 'Gửi email marketing với top 3 sản phẩm',
              description: 'Tạo campaign email personalized với sản phẩm được recommend',
              impact: 'Trung bình',
              effort: 'Thấp',
              priority: 'Cao'
            },
            {
              type: 'segmentation',
              title: 'Thêm vào nhóm khách hàng VIP',
              description: 'Phân loại khách hàng vào segment cao cấp để có ưu đãi riêng',
              impact: 'Thấp',
              effort: 'Thấp',
              priority: 'Trung bình'
            }
          ]
        };

      case 'review_sentiment':
        return {
          mlResults: {
            sentimentData: [
              { sentiment: 'Positive', count: 145, percentage: 58.2 },
              { sentiment: 'Negative', count: 67, percentage: 26.9 },
              { sentiment: 'Neutral', count: 37, percentage: 14.9 }
            ],
            totalReviews: 249,
            avgSentiment: 'Positive',
            sentimentScore: 0.68,
            keyPhrases: [
              { phrase: 'chất lượng tốt', sentiment: 'positive', frequency: 23 },
              { phrase: 'giao hàng nhanh', sentiment: 'positive', frequency: 18 },
              { phrase: 'giá hợp lý', sentiment: 'positive', frequency: 15 },
              { phrase: 'hỏng hóc', sentiment: 'negative', frequency: 12 },
              { phrase: 'chậm trễ', sentiment: 'negative', frequency: 8 }
            ]
          },
          aiSummary: {
            insights: [
              "58.2% đánh giá tích cực, cho thấy sản phẩm được đón nhận tốt",
              "Điểm sentiment trung bình là 0.68, nằm trong mức chấp nhận được",
              "Các từ khóa tích cực chủ yếu liên quan đến chất lượng và giao hàng"
            ],
            anomalies: [
              "Tăng 15% đánh giá tiêu cực trong tuần qua",
              "Một số review đề cập đến vấn đề chất lượng sản phẩm"
            ],
            risks: [
              "Rủi ro danh tiếng nếu không xử lý kịp thời các review tiêu cực",
              "Có thể ảnh hưởng đến tỷ lệ chuyển đổi nếu trend tiêu cực tiếp tục"
            ]
          },
          aiActions: [
            {
              type: 'response',
              title: 'Trả lời tất cả review tiêu cực trong 24h',
              description: 'Tạo response template và giao cho team CS xử lý ngay',
              impact: 'Cao',
              effort: 'Trung bình',
              priority: 'Cao'
            },
            {
              type: 'improvement',
              title: 'Kiểm tra chất lượng sản phẩm',
              description: 'Yêu cầu QC kiểm tra lô hàng mới để tránh vấn đề tương tự',
              impact: 'Cao',
              effort: 'Cao',
              priority: 'Cao'
            },
            {
              type: 'monitoring',
              title: 'Tăng cường monitor review',
              description: 'Thiết lập alert khi tỷ lệ review tiêu cực > 30%',
              impact: 'Thấp',
              effort: 'Thấp',
              priority: 'Trung bình'
            }
          ]
        };

      default:
        return {};
    }
  };

  const mockData = getMockData();

  // Use API data if available, otherwise fallback to mock
  const mlResults = dssResults || {};
  const insights = dssResults?.ai_summary_insights || [];
  const anomalies: string[] = []; // Not provided in DSS response
  const risks: string[] = []; // Not provided
  const recommendations = (dssResults?.ai_recommended_actions || []).map((action: string) => ({
    title: action,
    description: action,
    impact: 'Trung bình',
    effort: 'Trung bình',
    priority: 'Trung bình'
  }));

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('vi-VN', {
      style: 'currency',
      currency: 'VND',
    }).format(amount);
  };

  const getPriorityColor = (priority: string) => {
    switch (priority.toLowerCase()) {
      case 'cao': return 'bg-red-100 text-red-800';
      case 'trung bình': return 'bg-yellow-100 text-yellow-800';
      case 'thấp': return 'bg-green-100 text-green-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const renderMLResults = () => {
    switch (modelId) {
      case 'price_prediction':
        const priceData = mlResults as any; // PricePredictionResponse
        return (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="bg-blue-50 p-4 rounded-lg">
                <h4 className="font-medium text-blue-900">Tổng số sản phẩm</h4>
                <p className="text-2xl font-bold text-blue-600">{priceData?.kpi_summary?.num_products || 0}</p>
              </div>
              <div className="bg-green-50 p-4 rounded-lg">
                <h4 className="font-medium text-green-900">Sản phẩm có khuyến nghị</h4>
                <p className="text-2xl font-bold text-green-600">{priceData?.kpi_summary?.num_with_recommendation || 0}</p>
              </div>
              <div className="bg-purple-50 p-4 rounded-lg">
                <h4 className="font-medium text-purple-900">Tăng trưởng doanh thu dự kiến</h4>
                <p className="text-2xl font-bold text-purple-600">{priceData?.kpi_summary?.expected_revenue_uplift_pct ? `${priceData.kpi_summary.expected_revenue_uplift_pct.toFixed(1)}%` : 'N/A'}</p>
              </div>
            </div>
            <div className="bg-white p-4 rounded-lg border">
              <h4 className="font-medium mb-4">Khuyến nghị tối ưu hóa giá</h4>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Sản phẩm</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Giá hiện tại</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Giá dự đoán</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Thay đổi giá</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Tác động doanh thu</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Độ tin cậy</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {(priceData?.table_data as any[])?.slice(0, 10).map((item: any, index: number) => (
                      <tr key={index} className="hover:bg-gray-50">
                        <td className="px-6 py-4 whitespace-nowrap text-sm">
                          <button
                            onClick={() => navigate(`/analyst/product-review/${item.product_key}`)}
                            className="text-blue-600 hover:text-blue-800 hover:underline font-medium"
                          >
                            {item.product_name}
                          </button>
                          <div className="text-xs text-gray-500 mt-1">
                            Key: {item.product_key}
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {formatCurrency(item.current_price)}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {formatCurrency(item.predicted_price)}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm">
                          <span className={`px-2 py-1 text-xs rounded-full ${item.price_change_pct >= 0 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
                            {item.price_change_pct >= 0 ? '+' : ''}{item.price_change_pct.toFixed(1)}%
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {item.expected_revenue_change_pct >= 0 ? '+' : ''}{item.expected_revenue_change_pct.toFixed(1)}%
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {(item.confidence * 100).toFixed(1)}%
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        );

      case 'product_recommendation':
        const recoData = mlResults as any; // ProductRecommendationResponse
        return (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="bg-blue-50 p-4 rounded-lg">
                <h4 className="font-medium text-blue-900">Sản phẩm nguồn</h4>
                <p className="text-2xl font-bold text-blue-600">{recoData?.kpi_summary?.num_source_products || 0}</p>
              </div>
              <div className="bg-green-50 p-4 rounded-lg">
                <h4 className="font-medium text-green-900">Tổng số khuyến nghị</h4>
                <p className="text-2xl font-bold text-green-600">{recoData?.kpi_summary?.num_recommendations || 0}</p>
              </div>
              <div className="bg-purple-50 p-4 rounded-lg">
                <h4 className="font-medium text-purple-900">Độ tương đồng trung bình</h4>
                <p className="text-2xl font-bold text-purple-600">{recoData?.kpi_summary?.avg_similarity || 'N/A'}</p>
              </div>
            </div>
            <div className="bg-white rounded-lg border overflow-hidden">
              <div className="p-4 border-b">
                <h4 className="font-medium">Khuyến nghị sản phẩm</h4>
              </div>
              <div className="divide-y">
                {(recoData?.table_data as any[])?.map((rec: any, index: number) => (
                  <div key={index} className="p-4 hover:bg-gray-50">
                    <div className="flex justify-between items-start">
                      <div className="flex-1">
                        <div className="flex items-center gap-2 mb-1">
                          <span className="text-sm font-medium text-blue-600">Source:</span>
                          <span className="text-sm text-gray-900">{rec.source_product_name}</span>
                        </div>
                        <h5 className="font-medium">{rec.recommended_product_name}</h5>
                        <div className="flex gap-4 text-sm text-gray-600 mt-1">
                          <span>Similarity: {(parseFloat(rec.similarity_score) * 100).toFixed(1)}%</span>
                          <span>Type: {rec.recommendation_type}</span>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        );

      case 'review_sentiment':
        const sentimentData = mlResults as any; // ReviewSentimentResponse
        const COLORS = ['#10B981', '#EF4444', '#6B7280'];
        const sentimentChartData = [
          { name: 'Positive', value: sentimentData?.kpi_summary?.avg_positive_pct || 0, count: Math.round((sentimentData?.kpi_summary?.total_reviews || 0) * (sentimentData?.kpi_summary?.avg_positive_pct || 0) / 100) },
          { name: 'Negative', value: sentimentData?.kpi_summary?.avg_negative_pct || 0, count: Math.round((sentimentData?.kpi_summary?.total_reviews || 0) * (sentimentData?.kpi_summary?.avg_negative_pct || 0) / 100) },
          { name: 'Neutral', value: 100 - (sentimentData?.kpi_summary?.avg_positive_pct || 0) - (sentimentData?.kpi_summary?.avg_negative_pct || 0), count: Math.round((sentimentData?.kpi_summary?.total_reviews || 0) * (100 - (sentimentData?.kpi_summary?.avg_positive_pct || 0) - (sentimentData?.kpi_summary?.avg_negative_pct || 0)) / 100) }
        ];

        return (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="bg-blue-50 p-4 rounded-lg">
                <h4 className="font-medium text-blue-900">Tổng số đánh giá</h4>
                <p className="text-2xl font-bold text-blue-600">{sentimentData?.kpi_summary?.total_reviews || 0}</p>
              </div>
              <div className="bg-green-50 p-4 rounded-lg">
                <h4 className="font-medium text-green-900">Đánh giá tích cực</h4>
                <p className="text-2xl font-bold text-green-600">{sentimentData?.kpi_summary?.avg_positive_pct ? `${sentimentData.kpi_summary.avg_positive_pct.toFixed(1)}%` : 'N/A'}</p>
              </div>
              <div className="bg-red-50 p-4 rounded-lg">
                <h4 className="font-medium text-red-900">Sản phẩm quan trọng</h4>
                <p className="text-2xl font-bold text-red-600">{sentimentData?.kpi_summary?.num_products_with_critical_negative || 0}</p>
              </div>
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="bg-white p-4 rounded-lg border">
                <h4 className="font-medium mb-4">Phân bố cảm xúc</h4>
                <ResponsiveContainer width="100%" height={250}>
                  <PieChart>
                    <Pie
                      data={sentimentChartData}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ name, value }) => `${name}: ${value.toFixed(1)}%`}
                      outerRadius={80}
                      fill="#8884d8"
                      dataKey="value"
                    >
                      {sentimentChartData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value) => `${value}%`} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="bg-white p-4 rounded-lg border">
                <h4 className="font-medium mb-4">Phân tích sản phẩm hàng đầu</h4>
                <div className="space-y-3">
                  {(sentimentData?.table_data as any[])?.slice(0, 5).map((product: any, index: number) => (
                    <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded hover:bg-gray-100 cursor-pointer" onClick={() => navigate(`/analyst/product-review/${product.product_key}`)}>
                      <div className="flex-1">
                        <h5 className="font-medium text-sm text-blue-600 hover:text-blue-800 hover:underline">{product.product_name}</h5>
                        <p className="text-xs text-gray-600">{product.total_reviews} reviews</p>
                      </div>
                      <div className="text-right">
                        <div className={`text-sm font-medium ${product.positive_pct > 50 ? 'text-green-600' : product.negative_pct > 30 ? 'text-red-600' : 'text-yellow-600'}`}>
                          {product.positive_pct.toFixed(1)}% positive
                        </div>
                        <div className="text-xs text-gray-500">
                          {product.is_critical ? 'Critical' : 'Normal'}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        );

      default:
        return <div>Không có kết quả ML nào</div>;
    }
  };

  return (
    <div className="p-6">
      <div className="mb-6">
        <button
          onClick={() => navigate(`/analyst/dss/${modelId}`)}
          className="flex items-center text-blue-600 hover:text-blue-800 mb-4"
        >
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to DSS Input
        </button>
        <div className="flex items-center mb-4">
          <div className="p-3 bg-blue-100 dark:bg-blue-900/20 rounded-lg mr-4">
            {currentModel.icon}
          </div>
          <div>
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
              {currentModel.name} - Kết quả DSS
            </h1>
            <p className="text-gray-600 dark:text-gray-300">
              Thông tin kinh doanh và hiểu biết chiến lược được hỗ trợ bởi AI
            </p>
          </div>
        </div>
      </div>

      {/* ML Results Section */}
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4 flex items-center">
          <TrendingUp className="w-6 h-6 mr-2" />
          Kết quả ML & Trực quan hóa
        </h2>
        <div className="bg-gray-50 dark:bg-gray-800 p-6 rounded-lg">
          {renderMLResults()}
        </div>
      </div>

      {/* AI Summary Section */}
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4 flex items-center">
          <Lightbulb className="w-6 h-6 mr-2" />
          Tóm tắt Phân tích AI
        </h2>
        <div className="bg-blue-50 dark:bg-blue-900/20 p-6 rounded-lg">
          <div className="space-y-4">
            <div>
              <h3 className="font-semibold text-blue-900 dark:text-blue-100 mb-2">Những hiểu biết chính</h3>
              <ul className="list-disc list-inside space-y-1 text-blue-800 dark:text-blue-200">
                {insights.map((insight: string, index: number) => (
                  <li key={index}>{insight}</li>
                ))}
              </ul>
            </div>
            <div>
              <h3 className="font-semibold text-orange-900 dark:text-orange-100 mb-2 flex items-center">
                <AlertTriangle className="w-4 h-4 mr-1" />
                Các bất thường được phát hiện
              </h3>
              <ul className="list-disc list-inside space-y-1 text-orange-800 dark:text-orange-200">
                {anomalies.map((anomaly: string, index: number) => (
                  <li key={index}>{anomaly}</li>
                ))}
              </ul>
            </div>
            <div>
              <h3 className="font-semibold text-red-900 dark:text-red-100 mb-2">Đánh giá rủi ro</h3>
              <ul className="list-disc list-inside space-y-1 text-red-800 dark:text-red-200">
                {risks.map((risk: string, index: number) => (
                  <li key={index}>{risk}</li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      </div>

      {/* AI Action Recommendations Section */}
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4 flex items-center">
          <Target className="w-6 h-6 mr-2" />
          Khuyến nghị hành động của AI
        </h2>
        <div className="space-y-4">
          {recommendations.map((action: { title: string; description: string; impact: string; effort: string; priority: string }, index: number) => (
            <div key={index} className="bg-white dark:bg-gray-800 p-6 rounded-lg border border-gray-200 dark:border-gray-700">
              <div className="flex justify-between items-start mb-3">
                <h3 className="font-semibold text-gray-900 dark:text-white">{action.title}</h3>
                <div className="flex gap-2">
                  <span className={`px-2 py-1 text-xs rounded-full ${getPriorityColor(action.priority)}`}>
                    {action.priority}
                  </span>
                  <span className={`px-2 py-1 text-xs rounded-full ${action.impact === 'Cao' ? 'bg-green-100 text-green-800' : action.impact === 'Trung bình' ? 'bg-yellow-100 text-yellow-800' : 'bg-blue-100 text-blue-800'}`}>
                    Impact: {action.impact}
                  </span>
                </div>
              </div>
              <p className="text-gray-600 dark:text-gray-300 mb-3">{action.description}</p>
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-500">Effort: {action.effort}</span>
                <button className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors">
                  Thực hiện hành động
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default DSSResults;