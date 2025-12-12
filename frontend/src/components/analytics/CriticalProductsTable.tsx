import { AlertTriangle, ExternalLink } from 'lucide-react';
import { CriticalProduct } from '../../services/analyticsApi';

interface CriticalProductsTableProps {
    data: CriticalProduct[];
    title?: string;
}

export function CriticalProductsTable({
    data,
    title = 'Critical Products'
}: CriticalProductsTableProps) {
    if (!data || data.length === 0) {
        return (
            <div className="border border-gray-200 rounded-lg p-6 bg-white">
                <div className="flex items-center gap-2 mb-4">
                    <AlertTriangle className="h-5 w-5 text-red-500" />
                    <h3 className="font-semibold text-gray-900">{title}</h3>
                </div>
                <div className="text-center py-8 text-gray-500">
                    Không có sản phẩm critical
                </div>
            </div>
        );
    }

    return (
        <div className="border border-gray-200 rounded-lg bg-white">
            <div className="px-6 py-4 border-b border-gray-200 bg-red-50">
                <div className="flex items-center gap-2">
                    <AlertTriangle className="h-5 w-5 text-red-600" />
                    <h3 className="font-semibold text-gray-900">{title}</h3>
                    <span className="ml-auto text-sm text-gray-600">
                        {data.length} sản phẩm
                    </span>
                </div>
            </div>

            <div className="overflow-x-auto">
                <table className="w-full">
                    <thead className="bg-gray-50 border-b border-gray-200">
                        <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Product
                            </th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Platform
                            </th>
                            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Avg Rating
                            </th>
                            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Total Reviews
                            </th>
                            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Negative %
                            </th>
                            <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Action
                            </th>
                        </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                        {data.map((product, index) => (
                            <tr
                                key={`${product.product_key}-${index}`}
                                className="hover:bg-gray-50 transition-colors"
                            >
                                <td className="px-6 py-4">
                                    <div className="flex flex-col">
                                        <span className="text-sm font-medium text-gray-900 line-clamp-2">
                                            {product.product_name}
                                        </span>
                                        <span className="text-xs text-gray-500 mt-1">
                                            {product.category_name || 'Unknown'}
                                        </span>
                                    </div>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap">
                                    <span className={`inline-flex px-2.5 py-0.5 rounded-full text-xs font-medium ${product.platform_code === 'tiki'
                                            ? 'bg-blue-100 text-blue-800'
                                            : 'bg-purple-100 text-purple-800'
                                        }`}>
                                        {product.platform_code.toUpperCase()}
                                    </span>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-right">
                                    <div className="flex items-center justify-end gap-1">
                                        <span className={`text-sm font-semibold ${product.avg_rating < 2.0 ? 'text-red-600' :
                                                product.avg_rating < 3.0 ? 'text-orange-600' :
                                                    'text-yellow-600'
                                            }`}>
                                            {product.avg_rating.toFixed(1)}
                                        </span>
                                        <span className="text-yellow-500">⭐</span>
                                    </div>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-right">
                                    <span className="text-sm text-gray-900">
                                        {product.total_reviews.toLocaleString('vi-VN')}
                                    </span>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-right">
                                    <div className="flex items-center justify-end gap-2">
                                        <div className="w-16 bg-gray-200 rounded-full h-2">
                                            <div
                                                className={`h-2 rounded-full ${product.negative_pct >= 40 ? 'bg-red-600' :
                                                        product.negative_pct >= 25 ? 'bg-orange-500' :
                                                            'bg-yellow-500'
                                                    }`}
                                                style={{ width: `${Math.min(product.negative_pct, 100)}%` }}
                                            />
                                        </div>
                                        <span className={`text-sm font-medium ${product.negative_pct >= 40 ? 'text-red-600' :
                                                product.negative_pct >= 25 ? 'text-orange-600' :
                                                    'text-yellow-600'
                                            }`}>
                                            {product.negative_pct.toFixed(0)}%
                                        </span>
                                    </div>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-center">
                                    <button
                                        onClick={() => {
                                            // Navigate to product detail or DSS review sentiment
                                            console.log('View product:', product.product_key);
                                        }}
                                        className="text-indigo-600 hover:text-indigo-900 inline-flex items-center gap-1 text-sm font-medium"
                                    >
                                        Analyze <ExternalLink className="h-4 w-4" />
                                    </button>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>

            {data.length > 0 && (
                <div className="px-6 py-3 bg-gray-50 border-t border-gray-200">
                    <p className="text-xs text-gray-500">
                        💡 <strong>Tip:</strong> Products with rating {'<'} 3.5 or high negative % should be prioritized for review or DSS analysis.
                    </p>
                </div>
            )}
        </div>
    );
}
