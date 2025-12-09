// // Mock DSS and AI APIs for demonstration
// // In production, these would be real API calls

// export interface DSSRunRequest {
//   model_type: 'price_prediction' | 'product_recommendation' | 'review_sentiment';
//   input_data: Record<string, any>;
// }

// export interface DSSRunResponse {
//   model_results: Record<string, any>;
//   charts_data?: any[];
//   tables_data?: any[];
//   metrics?: Record<string, any>;
// }

// export interface AISummarizeRequest {
//   model_type: string;
//   ml_results: Record<string, any>;
//   business_context?: Record<string, any>;
// }

// export interface AISummarizeResponse {
//   summary: string;
//   insights: string[];
//   anomalies: string[];
//   risks: string[];
//   recommendations: {
//     title: string;
//     description: string;
//     impact: 'Cao' | 'Trung bình' | 'Thấp';
//     effort: 'Cao' | 'Trung bình' | 'Thấp';
//     priority: 'Cao' | 'Trung bình' | 'Thấp';
//   }[];
// }

// // Mock delay to simulate API calls
// const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// export const runDSSAnalysis = async (data: DSSRunRequest): Promise<DSSRunResponse> => {
//   await delay(1500); // Simulate network delay

//   switch (data.model_type) {
//     case 'price_prediction':
//       return {
//         model_results: {
//           currentPrice: 1200000,
//           predictedPrice: 1185000,
//           confidenceInterval: { lower: 1155000, upper: 1215000 },
//           accuracy: 92.5,
//           chartData: [
//             { date: '2025-11-01', price: 1200000, predicted: 1180000, lower: 1150000, upper: 1210000 },
//             { date: '2025-11-02', price: 1190000, predicted: 1175000, lower: 1145000, upper: 1205000 },
//             { date: '2025-11-03', price: 1210000, predicted: 1190000, lower: 1160000, upper: 1220000 },
//             { date: '2025-11-04', price: 1180000, predicted: 1165000, lower: 1135000, upper: 1195000 },
//             { date: '2025-11-05', price: 1220000, predicted: 1200000, lower: 1170000, upper: 1230000 },
//           ]
//         },
//         metrics: { mape: 7.5, r2_score: 0.925 }
//       };

//     case 'product_recommendation':
//       return {
//         model_results: {
//           totalProducts: 1247,
//           avgSimilarity: 0.84,
//           recommendations: [
//             { rank: 1, product_name: 'iPhone 15 Pro', similarity_score: 0.95, min_price: 25000000, avg_rating: 4.8 },
//             { rank: 2, product_name: 'Samsung Galaxy S24', similarity_score: 0.89, min_price: 22000000, avg_rating: 4.7 },
//             { rank: 3, product_name: 'Google Pixel 8', similarity_score: 0.82, min_price: 18000000, avg_rating: 4.5 },
//             { rank: 4, product_name: 'OnePlus 12', similarity_score: 0.78, min_price: 16000000, avg_rating: 4.4 },
//             { rank: 5, product_name: 'Xiaomi 14', similarity_score: 0.75, min_price: 14000000, avg_rating: 4.3 }
//           ]
//         },
//         metrics: { precision_at_5: 0.88, ndcg_score: 0.91 }
//       };

//     case 'review_sentiment':
//       return {
//         model_results: {
//           totalReviews: 249,
//           avgSentiment: 'Positive',
//           sentimentScore: 0.68,
//           sentimentData: [
//             { sentiment: 'Positive', count: 145, percentage: 58.2 },
//             { sentiment: 'Negative', count: 67, percentage: 26.9 },
//             { sentiment: 'Neutral', count: 37, percentage: 14.9 }
//           ],
//           keyPhrases: [
//             { phrase: 'chất lượng tốt', sentiment: 'positive', frequency: 23 },
//             { phrase: 'giao hàng nhanh', sentiment: 'positive', frequency: 18 },
//             { phrase: 'giá hợp lý', sentiment: 'positive', frequency: 15 },
//             { phrase: 'hỏng hóc', sentiment: 'negative', frequency: 12 },
//             { phrase: 'chậm trễ', sentiment: 'negative', frequency: 8 }
//           ]
//         },
//         metrics: { accuracy: 0.94, f1_score: 0.91 }
//       };

//     default:
//       throw new Error('Unsupported model type');
//   }
// };

// export const getAISummary = async (data: AISummarizeRequest): Promise<AISummarizeResponse> => {
//   await delay(1200); // Simulate AI processing delay

//   switch (data.model_type) {
//     case 'price_prediction':
//       return {
//         summary: "Phân tích dự đoán giá cho sản phẩm cho thấy xu hướng giảm nhẹ với độ tin cậy cao.",
//         insights: [
//           "Giá sản phẩm đang có xu hướng giảm nhẹ trong tuần qua",
//           "Mức giá dự đoán 1,185,000 VND nằm trong khoảng tin cậy 1,155,000 - 1,215,000 VND",
//           "Độ chính xác của mô hình là 92.5%, cho thấy dự đoán khá đáng tin cậy"
//         ],
//         anomalies: [
//           "Giá thực tế ngày 03/11 cao hơn dự kiến 2%, có thể do chương trình khuyến mãi đặc biệt"
//         ],
//         risks: [
//           "Rủi ro cạnh tranh giá từ đối thủ trên cùng platform",
//           "Thị trường có thể biến động nếu có sự kiện đặc biệt trong tháng 12"
//         ],
//         recommendations: [
//           {
//             title: 'Giảm giá 5% cho sản phẩm này',
//             description: 'Dựa trên dự đoán giá, đề xuất giảm 5% để tăng tính cạnh tranh',
//             impact: 'Cao',
//             effort: 'Thấp',
//             priority: 'Cao'
//           },
//           {
//             title: 'Tăng quảng cáo trên Tiki',
//             description: 'Tăng ngân sách quảng cáo 20% trong tuần tới để tận dụng xu hướng giá',
//             impact: 'Trung bình',
//             effort: 'Trung bình',
//             priority: 'Trung bình'
//           },
//           {
//             title: 'Điều chỉnh tồn kho',
//             description: 'Giảm nhập hàng 10% trong tháng tới để tránh tồn kho',
//             impact: 'Thấp',
//             effort: 'Thấp',
//             priority: 'Thấp'
//           }
//         ]
//       };

//     case 'product_recommendation':
//       return {
//         summary: "Hệ thống đề xuất sản phẩm đã phân tích và đưa ra 5 sản phẩm tương tự với độ chính xác cao.",
//         insights: [
//           "Khách hàng có sở thích mạnh với smartphone cao cấp (iPhone, Samsung)",
//           "Độ tương đồng cao nhất là 95% với iPhone 15 Pro",
//           "Trung bình độ tương đồng của top 5 sản phẩm là 84%"
//         ],
//         anomalies: [
//           "Khách hàng này chưa mua smartphone trong 6 tháng qua",
//           "Có xu hướng tìm kiếm sản phẩm giá cao hơn mức trung bình"
//         ],
//         risks: [
//           "Rủi ro khách hàng hủy đơn nếu giá thay đổi",
//           "Cạnh tranh từ flash sale có thể làm giảm tỷ lệ chuyển đổi"
//         ],
//         recommendations: [
//           {
//             title: 'Gửi voucher 500k cho iPhone 15 Pro',
//             description: 'Tạo ưu đãi đặc biệt cho sản phẩm được recommend cao nhất',
//             impact: 'Cao',
//             effort: 'Thấp',
//             priority: 'Cao'
//           },
//           {
//             title: 'Gửi email marketing với top 3 sản phẩm',
//             description: 'Tạo campaign email personalized với sản phẩm được recommend',
//             impact: 'Trung bình',
//             effort: 'Thấp',
//             priority: 'Cao'
//           },
//           {
//             title: 'Thêm vào nhóm khách hàng VIP',
//             description: 'Phân loại khách hàng vào segment cao cấp để có ưu đãi riêng',
//             impact: 'Thấp',
//             effort: 'Thấp',
//             priority: 'Trung bình'
//           }
//         ]
//       };

//     case 'review_sentiment':
//       return {
//         summary: "Phân tích cảm xúc cho thấy 58.2% đánh giá tích cực với một số vấn đề cần lưu ý.",
//         insights: [
//           "58.2% đánh giá tích cực, cho thấy sản phẩm được đón nhận tốt",
//           "Điểm sentiment trung bình là 0.68, nằm trong mức chấp nhận được",
//           "Các từ khóa tích cực chủ yếu liên quan đến chất lượng và giao hàng"
//         ],
//         anomalies: [
//           "Tăng 15% đánh giá tiêu cực trong tuần qua",
//           "Một số review đề cập đến vấn đề chất lượng sản phẩm"
//         ],
//         risks: [
//           "Rủi ro danh tiếng nếu không xử lý kịp thời các review tiêu cực",
//           "Có thể ảnh hưởng đến tỷ lệ chuyển đổi nếu trend tiêu cực tiếp tục"
//         ],
//         recommendations: [
//           {
//             title: 'Trả lời tất cả review tiêu cực trong 24h',
//             description: 'Tạo response template và giao cho team CS xử lý ngay',
//             impact: 'Cao',
//             effort: 'Trung bình',
//             priority: 'Cao'
//           },
//           {
//             title: 'Kiểm tra chất lượng sản phẩm',
//             description: 'Yêu cầu QC kiểm tra lô hàng mới để tránh vấn đề tương tự',
//             impact: 'Cao',
//             effort: 'Cao',
//             priority: 'Cao'
//           },
//           {
//             title: 'Tăng cường monitor review',
//             description: 'Thiết lập alert khi tỷ lệ review tiêu cực > 30%',
//             impact: 'Thấp',
//             effort: 'Thấp',
//             priority: 'Trung bình'
//           }
//         ]
//       };

//     default:
//       throw new Error('Unsupported model type');
//   }
// };