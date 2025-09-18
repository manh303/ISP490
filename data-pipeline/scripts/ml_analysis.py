# File: scripts/ml_analysis.py
#!/usr/bin/env python3

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_squared_error, classification_report, silhouette_score
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from loguru import logger
import joblib
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = Path("data")
MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)

class MLAnalyzer:
    def __init__(self):
        self.data_dir = DATA_DIR
        self.model_dir = MODEL_DIR
        self.customers_df = None
        self.products_df = None
        self.transactions_df = None
        self.ml_dataset = None
        
    def load_and_prepare_data(self):
        """Load và chuẩn bị dữ liệu cho ML"""
        logger.info("📖 Chuẩn bị dữ liệu cho Machine Learning...")
        
        # Load data
        self.customers_df = pd.read_csv(self.data_dir / 'customers.csv')
        self.products_df = pd.read_csv(self.data_dir / 'products.csv')
        self.transactions_df = pd.read_csv(self.data_dir / 'transactions.csv')
        
        # Convert dates
        self.customers_df['registration_date'] = pd.to_datetime(self.customers_df['registration_date'])
        self.products_df['created_date'] = pd.to_datetime(self.products_df['created_date'])
        self.transactions_df['order_date'] = pd.to_datetime(self.transactions_df['order_date'])
        
        # Tạo dataset tổng hợp cho ML
        self.create_ml_features()
        logger.success("✅ Dữ liệu đã sẵn sàng cho ML")
        
    def create_ml_features(self):
        """Tạo features cho machine learning"""
        logger.info("🔧 Tạo features cho ML...")
        
        # Customer features
        customer_features = self.customers_df.copy()
        customer_features['days_since_registration'] = (
            pd.Timestamp.now() - customer_features['registration_date']
        ).dt.days
        
        # Transaction aggregations per customer
        customer_transactions = (self.transactions_df.groupby('customer_id').agg({
            'item_total': ['sum', 'mean', 'count'],
            'quantity': ['sum', 'mean'],
            'order_id': 'nunique',
            'order_date': ['min', 'max']
        }).round(2))
        
        # Flatten column names
        customer_transactions.columns = [
            'total_spent', 'avg_order_value', 'total_transactions',
            'total_quantity', 'avg_quantity_per_transaction',
            'unique_orders', 'first_purchase', 'last_purchase'
        ]
        
        customer_transactions['days_since_first_purchase'] = (
            pd.Timestamp.now() - pd.to_datetime(customer_transactions['first_purchase'])
        ).dt.days
        
        customer_transactions['days_since_last_purchase'] = (
            pd.Timestamp.now() - pd.to_datetime(customer_transactions['last_purchase'])
        ).dt.days
        
        customer_transactions['purchase_frequency'] = (
            customer_transactions['unique_orders'] / 
            (customer_transactions['days_since_first_purchase'] + 1) * 30  # orders per month
        ).round(3)
        
        # Product category preferences
        product_categories = (self.transactions_df
                             .merge(self.products_df[['product_id', 'category']], on='product_id')
                             .groupby(['customer_id', 'category'])['quantity'].sum()
                             .unstack(fill_value=0))
        
        product_categories.columns = [f'category_{col.lower().replace(" ", "_")}' for col in product_categories.columns]
        
        # Merge all features
        self.ml_dataset = (customer_features
                          .merge(customer_transactions, on='customer_id', how='left')
                          .merge(product_categories, on='customer_id', how='left'))
        
        # Fill NaN values
        self.ml_dataset = self.ml_dataset.fillna(0)
        
        # Encode categorical variables
        le_country = LabelEncoder()
        le_segment = LabelEncoder()
        le_gender = LabelEncoder()
        
        self.ml_dataset['country_encoded'] = le_country.fit_transform(self.ml_dataset['country'])
        self.ml_dataset['segment_encoded'] = le_segment.fit_transform(self.ml_dataset['customer_segment'])
        self.ml_dataset['gender_encoded'] = le_gender.fit_transform(self.ml_dataset['gender'])
        
        logger.info(f"📊 Dataset shape: {self.ml_dataset.shape}")
        logger.info(f"📋 Features: {list(self.ml_dataset.columns[:10])}...")
        
    def customer_lifetime_value_prediction(self):
        """Dự đoán Customer Lifetime Value"""
        logger.info("🔮 Huấn luyện mô hình dự đoán CLV...")
        
        # Prepare features
        feature_cols = [
            'age', 'days_since_registration', 'total_transactions', 'avg_order_value',
            'total_quantity', 'avg_quantity_per_transaction', 'unique_orders',
            'days_since_first_purchase', 'days_since_last_purchase', 'purchase_frequency',
            'country_encoded', 'segment_encoded', 'gender_encoded'
        ]
        
        # Add category features
        category_cols = [col for col in self.ml_dataset.columns if col.startswith('category_')]
        feature_cols.extend(category_cols)
        
        X = self.ml_dataset[feature_cols].fillna(0)
        y = self.ml_dataset['lifetime_value']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train model
        rf_model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        rf_model.fit(X_train, y_train)
        
        # Predictions
        y_pred = rf_model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': rf_model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        # Visualize results
        plt.figure(figsize=(15, 5))
        
        plt.subplot(1, 3, 1)
        plt.scatter(y_test, y_pred, alpha=0.6)
        plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')
        plt.xlabel('Actual CLV')
        plt.ylabel('Predicted CLV')
        plt.title(f'🎯 CLV Prediction\nRMSE: {rmse:.2f}')
        
        plt.subplot(1, 3, 2)
        top_features = feature_importance.head(10)
        plt.barh(range(len(top_features)), top_features['importance'])
        plt.yticks(range(len(top_features)), top_features['feature'])
        plt.xlabel('Feature Importance')
        plt.title('📊 Top Features for CLV')
        
        plt.subplot(1, 3, 3)
        plt.hist(y_pred - y_test, bins=30, alpha=0.7)
        plt.xlabel('Prediction Error')
        plt.ylabel('Frequency')
        plt.title('📈 Prediction Error Distribution')
        
        plt.tight_layout()
        plt.savefig('charts/clv_prediction.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # Save model
        joblib.dump(rf_model, self.model_dir / 'clv_model.pkl')
        feature_importance.to_csv(self.model_dir / 'clv_feature_importance.csv', index=False)
        
        logger.success(f"✅ CLV Model - RMSE: {rmse:.2f}")
        return rf_model, feature_importance
    
    def customer_segmentation_clustering(self):
        """Phân khúc khách hàng bằng clustering"""
        logger.info("🎯 Phân khúc khách hàng với K-Means...")
        
        # Features for clustering
        cluster_features = [
            'total_spent', 'avg_order_value', 'total_transactions',
            'purchase_frequency', 'days_since_last_purchase', 'age'
        ]
        
        X_cluster = self.ml_dataset[cluster_features].fillna(0)
        
        # Standardize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_cluster)
        
        # Find optimal number of clusters
        inertias = []
        silhouette_scores = []
        K_range = range(2, 11)
        
        for k in K_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            kmeans.fit(X_scaled)
            inertias.append(kmeans.inertia_)
            silhouette_scores.append(silhouette_score(X_scaled, kmeans.labels_))
        
        # Choose optimal k (highest silhouette score)
        optimal_k = K_range[np.argmax(silhouette_scores)]
        logger.info(f"🎯 Optimal number of clusters: {optimal_k}")
        
        # Final clustering
        kmeans_final = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
        cluster_labels = kmeans_final.fit_predict(X_scaled)
        
        # Add clusters to dataset
        self.ml_dataset['ml_cluster'] = cluster_labels
        
        # Analyze clusters
        cluster_analysis = (self.ml_dataset.groupby('ml_cluster')[cluster_features]
                           .mean().round(2))
        
        # Visualize clustering
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # Elbow method
        axes[0, 0].plot(K_range, inertias, 'bo-')
        axes[0, 0].set_xlabel('Number of Clusters (k)')
        axes[0, 0].set_ylabel('Inertia')
        axes[0, 0].set_title('📊 Elbow Method')
        axes[0, 0].axvline(x=optimal_k, color='red', linestyle='--', label=f'Optimal k={optimal_k}')
        axes[0, 0].legend()
        
        # Silhouette scores
        axes[0, 1].plot(K_range, silhouette_scores, 'ro-')
        axes[0, 1].set_xlabel('Number of Clusters (k)')
        axes[0, 1].set_ylabel('Silhouette Score')
        axes[0, 1].set_title('📈 Silhouette Analysis')
        axes[0, 1].axvline(x=optimal_k, color='red', linestyle='--')
        
        # Cluster scatter plots
        scatter_x = self.ml_dataset['total_spent']
        scatter_y = self.ml_dataset['purchase_frequency']
        scatter = axes[1, 0].scatter(scatter_x, scatter_y, c=cluster_labels, cmap='viridis', alpha=0.6)
        axes[1, 0].set_xlabel('Total Spent')
        axes[1, 0].set_ylabel('Purchase Frequency')
        axes[1, 0].set_title('🎨 Customer Clusters')
        plt.colorbar(scatter, ax=axes[1, 0])
        
        # Cluster sizes
        cluster_sizes = pd.Series(cluster_labels).value_counts().sort_index()
        axes[1, 1].pie(cluster_sizes.values, labels=[f'Cluster {i}' for i in cluster_sizes.index], 
                      autopct='%1.1f%%')
        axes[1, 1].set_title('🥧 Cluster Distribution')
        
        plt.tight_layout()
        plt.savefig('charts/customer_clustering.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # Save results
        joblib.dump(kmeans_final, self.model_dir / 'customer_clustering_model.pkl')
        joblib.dump(scaler, self.model_dir / 'clustering_scaler.pkl')
        cluster_analysis.to_csv(self.model_dir / 'cluster_analysis.csv')
        
        logger.success(f"✅ Customer Clustering - {optimal_k} clusters created")
        return kmeans_final, cluster_analysis
    
    def churn_prediction(self):
        """Dự đoán khách hàng churn"""
        logger.info("⚠️ Huấn luyện mô hình dự đoán churn...")
        
        # Define churn (customers who haven't purchased in last 90 days)
        self.ml_dataset['is_churned'] = (self.ml_dataset['days_since_last_purchase'] > 90).astype(int)
        
        churn_rate = self.ml_dataset['is_churned'].mean()
        logger.info(f"📊 Churn rate: {churn_rate:.2%}")
        
        # Features for churn prediction
        churn_features = [
            'age', 'days_since_registration', 'total_spent', 'avg_order_value',
            'total_transactions', 'purchase_frequency', 'days_since_last_purchase',
            'country_encoded', 'segment_encoded', 'gender_encoded'
        ]
        
        X_churn = self.ml_dataset[churn_features].fillna(0)
        y_churn = self.ml_dataset['is_churned']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X_churn, y_churn, test_size=0.2, 
                                                           random_state=42, stratify=y_churn)
        
        # Train model
        rf_churn = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
        rf_churn.fit(X_train, y_train)
        
        # Predictions
        y_pred_churn = rf_churn.predict(X_test)
        y_pred_proba = rf_churn.predict_proba(X_test)[:, 1]
        
        # Evaluation
        churn_report = classification_report(y_test, y_pred_churn, output_dict=True)
        
        # Feature importance
        churn_importance = pd.DataFrame({
            'feature': churn_features,
            'importance': rf_churn.feature_importances_
        }).sort_values('importance', ascending=False)
        
        # Visualize churn analysis
        plt.figure(figsize=(15, 5))
        
        plt.subplot(1, 3, 1)
        plt.hist([y_pred_proba[y_test == 0], y_pred_proba[y_test == 1]], 
                bins=20, alpha=0.7, label=['Not Churned', 'Churned'])
        plt.xlabel('Churn Probability')
        plt.ylabel('Frequency')
        plt.title('🎯 Churn Probability Distribution')
        plt.legend()
        
        plt.subplot(1, 3, 2)
        top_churn_features = churn_importance.head(8)
        plt.barh(range(len(top_churn_features)), top_churn_features['importance'])
        plt.yticks(range(len(top_churn_features)), top_churn_features['feature'])
        plt.xlabel('Feature Importance')
        plt.title('📊 Churn Prediction Features')
        
        plt.subplot(1, 3, 3)
        churn_by_segment = self.ml_dataset.groupby('customer_segment')['is_churned'].mean()
        plt.bar(churn_by_segment.index, churn_by_segment.values, color='coral')
        plt.xlabel('Customer Segment')
        plt.ylabel('Churn Rate')
        plt.title('📈 Churn Rate by Segment')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig('charts/churn_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # Save model
        joblib.dump(rf_churn, self.model_dir / 'churn_model.pkl')
        churn_importance.to_csv(self.model_dir / 'churn_feature_importance.csv', index=False)
        
        accuracy = churn_report['accuracy']
        logger.success(f"✅ Churn Model - Accuracy: {accuracy:.3f}")
        
        return rf_churn, churn_importance
    
    def recommendation_system(self):
        """Hệ thống gợi ý sản phẩm đơn giản"""
        logger.info("🛍️ Tạo hệ thống gợi ý sản phẩm...")
        
        # Tạo ma trận customer-product
        customer_product_matrix = (self.transactions_df.groupby(['customer_id', 'product_id'])
                                  .agg({'quantity': 'sum'})
                                  .unstack(fill_value=0))
        
        customer_product_matrix.columns = customer_product_matrix.columns.droplevel(0)
        
        # Tính similarity giữa customers dựa trên products đã mua
        from sklearn.metrics.pairwise import cosine_similarity
        
        customer_similarity = cosine_similarity(customer_product_matrix.values)
        customer_similarity_df = pd.DataFrame(
            customer_similarity, 
            index=customer_product_matrix.index,
            columns=customer_product_matrix.index
        )
        
        # Top products by category
        product_recommendations = {}
        for category in self.products_df['category'].unique():
            category_products = (self.transactions_df
                               .merge(self.products_df[['product_id', 'category']], on='product_id')
                               .query(f"category == '{category}'")
                               .groupby('product_id')
                               .agg({'quantity': 'sum', 'item_total': 'sum'})
                               .assign(score=lambda x: x['quantity'] * 0.3 + x['item_total'] * 0.7)
                               .sort_values('score', ascending=False)
                               .head(5))
            
            product_recommendations[category] = category_products.index.tolist()
        
        # Customer purchase patterns
        customer_patterns = (self.transactions_df
                           .merge(self.products_df[['product_id', 'category']], on='product_id')
                           .groupby(['customer_id', 'category'])
                           .agg({'quantity': 'sum'})
                           .unstack(fill_value=0))
        
        customer_patterns.columns = customer_patterns.columns.droplevel(0)
        
        # Save recommendation data
        import pickle
        
        recommendation_data = {
            'customer_similarity': customer_similarity_df,
            'product_recommendations': product_recommendations,
            'customer_patterns': customer_patterns,
            'customer_product_matrix': customer_product_matrix
        }
        
        with open(self.model_dir / 'recommendation_system.pkl', 'wb') as f:
            pickle.dump(recommendation_data, f)
        
        # Visualize recommendation insights
        plt.figure(figsize=(15, 10))
        
        # Top categories by volume
        plt.subplot(2, 2, 1)
        category_sales = (self.transactions_df
                         .merge(self.products_df[['product_id', 'category']], on='product_id')
                         .groupby('category')['quantity'].sum()
                         .sort_values(ascending=True))
        plt.barh(range(len(category_sales)), category_sales.values, color='skyblue')
        plt.yticks(range(len(category_sales)), category_sales.index)
        plt.xlabel('Total Quantity Sold')
        plt.title('📊 Sales Volume by Category')
        
        # Customer category preferences heatmap
        plt.subplot(2, 2, 2)
        # Sample top 20 customers for visualization
        top_customers = self.ml_dataset.nlargest(20, 'total_spent')['customer_id']
        sample_patterns = customer_patterns.loc[customer_patterns.index.isin(top_customers)]
        
        sns.heatmap(sample_patterns.iloc[:10], annot=True, fmt='.0f', cmap='Blues')
        plt.title('🔥 Customer Category Heatmap')
        plt.xlabel('Product Categories')
        plt.ylabel('Customers (Top 10)')
        
        # Product popularity distribution
        plt.subplot(2, 2, 3)
        product_popularity = (self.transactions_df.groupby('product_id')['quantity']
                            .sum().sort_values(ascending=False))
        plt.hist(product_popularity.values, bins=50, color='lightgreen', alpha=0.7)
        plt.xlabel('Total Quantity Sold')
        plt.ylabel('Number of Products')
        plt.title('📈 Product Popularity Distribution')
        plt.yscale('log')
        
        # Cross-sell opportunities
        plt.subplot(2, 2, 4)
        category_combinations = (self.transactions_df
                               .merge(self.products_df[['product_id', 'category']], on='product_id')
                               .groupby('order_id')['category']
                               .apply(lambda x: len(x.unique()))
                               .value_counts()
                               .sort_index())
        
        plt.bar(category_combinations.index, category_combinations.values, color='orange', alpha=0.7)
        plt.xlabel('Number of Categories per Order')
        plt.ylabel('Number of Orders')
        plt.title('🛒 Cross-sell Analysis')
        
        plt.tight_layout()
        plt.savefig('charts/recommendation_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        logger.success("✅ Recommendation system created")
        return recommendation_data
    
    def sales_forecasting(self):
        """Dự báo doanh số"""
        logger.info("📊 Dự báo doanh số...")
        
        # Tạo dữ liệu doanh số theo ngày
        daily_sales = (self.transactions_df
                      .groupby('order_date')['item_total']
                      .sum()
                      .sort_index())
        
        # Thêm features time series
        sales_df = daily_sales.reset_index()
        sales_df['day_of_week'] = sales_df['order_date'].dt.dayofweek
        sales_df['month'] = sales_df['order_date'].dt.month
        sales_df['day_of_month'] = sales_df['order_date'].dt.day
        sales_df['is_weekend'] = sales_df['day_of_week'].isin([5, 6]).astype(int)
        
        # Rolling averages
        sales_df['sales_7d_avg'] = sales_df['item_total'].rolling(7, min_periods=1).mean()
        sales_df['sales_30d_avg'] = sales_df['item_total'].rolling(30, min_periods=1).mean()
        
        # Lag features
        sales_df['sales_lag1'] = sales_df['item_total'].shift(1)
        sales_df['sales_lag7'] = sales_df['item_total'].shift(7)
        
        # Prepare for forecasting
        feature_cols = ['day_of_week', 'month', 'day_of_month', 'is_weekend', 
                       'sales_7d_avg', 'sales_30d_avg', 'sales_lag1', 'sales_lag7']
        
        # Remove rows with NaN
        forecast_df = sales_df.dropna()
        
        if len(forecast_df) > 50:  # Enough data for forecasting
            X_forecast = forecast_df[feature_cols]
            y_forecast = forecast_df['item_total']
            
            # Split for time series (last 20% for testing)
            split_point = int(len(forecast_df) * 0.8)
            X_train = X_forecast.iloc[:split_point]
            X_test = X_forecast.iloc[split_point:]
            y_train = y_forecast.iloc[:split_point]
            y_test = y_forecast.iloc[split_point:]
            
            # Train forecasting model
            forecast_model = RandomForestRegressor(n_estimators=100, random_state=42)
            forecast_model.fit(X_train, y_train)
            
            # Predictions
            y_pred_forecast = forecast_model.predict(X_test)
            forecast_mse = mean_squared_error(y_test, y_pred_forecast)
            forecast_rmse = np.sqrt(forecast_mse)
            
            # Visualize forecasting
            plt.figure(figsize=(15, 10))
            
            plt.subplot(2, 2, 1)
            plt.plot(forecast_df.index[:split_point], y_train, label='Training', color='blue')
            plt.plot(forecast_df.index[split_point:], y_test, label='Actual', color='green')
            plt.plot(forecast_df.index[split_point:], y_pred_forecast, label='Predicted', color='red', linestyle='--')
            plt.xlabel('Days')
            plt.ylabel('Daily Sales')
            plt.title(f'📈 Sales Forecasting\nRMSE: {forecast_rmse:,.0f}')
            plt.legend()
            
            # Feature importance for forecasting
            plt.subplot(2, 2, 2)
            forecast_importance = pd.DataFrame({
                'feature': feature_cols,
                'importance': forecast_model.feature_importances_
            }).sort_values('importance', ascending=True)
            
            plt.barh(range(len(forecast_importance)), forecast_importance['importance'])
            plt.yticks(range(len(forecast_importance)), forecast_importance['feature'])
            plt.xlabel('Feature Importance')
            plt.title('📊 Forecasting Feature Importance')
            
            # Sales by day of week
            plt.subplot(2, 2, 3)
            dow_sales = sales_df.groupby('day_of_week')['item_total'].mean()
            dow_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            plt.bar(range(7), dow_sales.values, color='lightblue')
            plt.xticks(range(7), dow_names)
            plt.xlabel('Day of Week')
            plt.ylabel('Average Daily Sales')
            plt.title('📅 Sales Pattern by Day')
            
            # Monthly sales trend
            plt.subplot(2, 2, 4)
            monthly_trend = sales_df.groupby('month')['item_total'].mean()
            plt.plot(monthly_trend.index, monthly_trend.values, marker='o', color='purple')
            plt.xlabel('Month')
            plt.ylabel('Average Daily Sales')
            plt.title('📊 Monthly Sales Trend')
            plt.xticks(range(1, 13))
            
            plt.tight_layout()
            plt.savefig('charts/sales_forecasting.png', dpi=300, bbox_inches='tight')
            plt.show()
            
            # Save forecasting model
            joblib.dump(forecast_model, self.model_dir / 'sales_forecast_model.pkl')
            
            logger.success(f"✅ Sales Forecasting - RMSE: {forecast_rmse:,.0f}")
            return forecast_model
        
        else:
            logger.warning("⚠️ Không đủ dữ liệu để dự báo")
            return None
    
    def generate_ml_report(self):
        """Tạo báo cáo ML tổng hợp"""
        logger.info("📋 Tạo báo cáo Machine Learning...")
        
        # Collect model performance metrics
        model_files = list(self.model_dir.glob("*.pkl"))
        
        ml_summary = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'dataset_info': {
                'total_customers': len(self.ml_dataset),
                'total_features': len(self.ml_dataset.columns),
                'date_range': f"{self.transactions_df['order_date'].min()} to {self.transactions_df['order_date'].max()}"
            },
            'models_created': [f.stem for f in model_files],
            'ml_insights': {
                'churn_rate': f"{self.ml_dataset['is_churned'].mean():.2%}",
                'high_value_customers': len(self.ml_dataset[self.ml_dataset['total_spent'] > self.ml_dataset['total_spent'].quantile(0.9)]),
                'top_customer_cluster': int(self.ml_dataset['ml_cluster'].value_counts().index[0]),
                'avg_clv': f"{self.ml_dataset['lifetime_value'].mean():.0f}"
            }
        }
        
        # HTML Report
        html_content = f"""
        <!DOCTYPE html>
        <html lang="vi">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>🤖 Machine Learning Analysis Report</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    margin: 0;
                    padding: 20px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: #333;
                    min-height: 100vh;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: rgba(255,255,255,0.95);
                    border-radius: 15px;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.2);
                    overflow: hidden;
                }}
                .header {{
                    background: linear-gradient(135deg, #ff6b6b 0%, #ee5a52 100%);
                    color: white;
                    padding: 40px;
                    text-align: center;
                }}
                .header h1 {{
                    margin: 0;
                    font-size: 2.5em;
                    font-weight: 300;
                }}
                .content {{
                    padding: 40px;
                }}
                .model-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 25px;
                    margin: 30px 0;
                }}
                .model-card {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 25px;
                    border-radius: 15px;
                    box-shadow: 0 10px 20px rgba(0,0,0,0.1);
                    transform: translateY(0);
                    transition: transform 0.3s ease;
                }}
                .model-card:hover {{
                    transform: translateY(-5px);
                }}
                .model-title {{
                    font-size: 1.5em;
                    font-weight: bold;
                    margin-bottom: 15px;
                    display: flex;
                    align-items: center;
                }}
                .model-title .icon {{
                    margin-right: 10px;
                    font-size: 1.2em;
                }}
                .chart-gallery {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
                    gap: 30px;
                    margin: 40px 0;
                }}
                .chart-item {{
                    background: white;
                    border-radius: 15px;
                    overflow: hidden;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.1);
                }}
                .chart-item img {{
                    width: 100%;
                    height: auto;
                    display: block;
                }}
                .chart-title {{
                    padding: 20px;
                    background: #f8f9fa;
                    font-weight: bold;
                    text-align: center;
                    color: #495057;
                }}
                .insights {{
                    background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%);
                    border-radius: 15px;
                    padding: 30px;
                    margin: 30px 0;
                }}
                .insights h3 {{
                    color: #8b4513;
                    margin-top: 0;
                    font-size: 1.5em;
                }}
                .metric {{
                    display: inline-block;
                    background: rgba(255,255,255,0.8);
                    padding: 15px 20px;
                    border-radius: 10px;
                    margin: 10px;
                    box-shadow: 0 5px 15px rgba(0,0,0,0.1);
                }}
                .metric-value {{
                    font-size: 1.5em;
                    font-weight: bold;
                    color: #8b4513;
                }}
                .metric-label {{
                    font-size: 0.9em;
                    color: #666;
                    margin-top: 5px;
                }}
                .footer {{
                    background: #2c3e50;
                    color: white;
                    text-align: center;
                    padding: 30px;
                }}
                .pulse {{
                    animation: pulse 2s infinite;
                }}
                @keyframes pulse {{
                    0% {{ opacity: 1; }}
                    50% {{ opacity: 0.7; }}
                    100% {{ opacity: 1; }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🤖 MACHINE LEARNING ANALYSIS</h1>
                    <p class="pulse">Advanced Analytics & Predictive Models</p>
                    <p>{pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}</p>
                </div>
                
                <div class="content">
                    <div class="model-grid">
                        <div class="model-card">
                            <div class="model-title">
                                <span class="icon">🔮</span>
                                CLV Prediction
                            </div>
                            <p>Dự đoán giá trị lifetime của khách hàng sử dụng Random Forest</p>
                            <strong>Status:</strong> ✅ Trained & Ready
                        </div>
                        
                        <div class="model-card">
                            <div class="model-title">
                                <span class="icon">🎯</span>
                                Customer Segmentation
                            </div>
                            <p>Phân khúc khách hàng tự động bằng K-Means Clustering</p>
                            <strong>Status:</strong> ✅ Trained & Ready
                        </div>
                        
                        <div class="model-card">
                            <div class="model-title">
                                <span class="icon">⚠️</span>
                                Churn Prediction
                            </div>
                            <p>Dự đoán khách hàng có nguy cơ rời bỏ</p>
                            <strong>Status:</strong> ✅ Trained & Ready
                        </div>
                        
                        <div class="model-card">
                            <div class="model-title">
                                <span class="icon">🛍️</span>
                                Recommendation System
                            </div>
                            <p>Gợi ý sản phẩm dựa trên hành vi mua sắm</p>
                            <strong>Status:</strong> ✅ Trained & Ready
                        </div>
                        
                        <div class="model-card">
                            <div class="model-title">
                                <span class="icon">📊</span>
                                Sales Forecasting
                            </div>
                            <p>Dự báo doanh số theo thời gian</p>
                            <strong>Status:</strong> ✅ Trained & Ready
                        </div>
                    </div>
                    
                    <div class="insights">
                        <h3>💡 Key ML Insights</h3>
                        <div class="metric">
                            <div class="metric-value">{ml_summary['ml_insights']['churn_rate']}</div>
                            <div class="metric-label">Churn Rate</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">{ml_summary['ml_insights']['high_value_customers']}</div>
                            <div class="metric-label">High-Value Customers</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">{ml_summary['ml_insights']['avg_clv']}</div>
                            <div class="metric-label">Average CLV</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">{len(ml_summary['models_created'])}</div>
                            <div class="metric-label">ML Models Created</div>
                        </div>
                    </div>
                    
                    <div class="chart-gallery">
                        <div class="chart-item">
                            <div class="chart-title">🔮 Customer Lifetime Value Prediction</div>
                            <img src="charts/clv_prediction.png" alt="CLV Prediction">
                        </div>
                        <div class="chart-item">
                            <div class="chart-title">🎯 Customer Segmentation</div>
                            <img src="charts/customer_clustering.png" alt="Customer Clustering">
                        </div>
                        <div class="chart-item">
                            <div class="chart-title">⚠️ Churn Analysis</div>
                            <img src="charts/churn_analysis.png" alt="Churn Analysis">
                        </div>
                        <div class="chart-item">
                            <div class="chart-title">🛍️ Recommendation Insights</div>
                            <img src="charts/recommendation_analysis.png" alt="Recommendation Analysis">
                        </div>
                        <div class="chart-item">
                            <div class="chart-title">📊 Sales Forecasting</div>
                            <img src="charts/sales_forecasting.png" alt="Sales Forecasting">
                        </div>
                    </div>
                </div>
                
                <div class="footer">
                    <p>🚀 Powered by Advanced Machine Learning</p>
                    <p>📁 Models saved in: ./models/</p>
                    <p>🎯 Ready for Production Deployment</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        with open('ml_analysis_report.html', 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # Save summary
        import json
        with open(self.model_dir / 'ml_summary.json', 'w', encoding='utf-8') as f:
            json.dump(ml_summary, f, indent=2, ensure_ascii=False)
        
        logger.success("✅ ML Report generated: ml_analysis_report.html")
        
    def run_complete_ml_analysis(self):
        """Chạy phân tích ML hoàn chỉnh"""
        logger.info("🚀 Bắt đầu Machine Learning Analysis")
        
        # Load and prepare data
        self.load_and_prepare_data()
        
        # Run all ML models
        logger.info("🔮 Step 1: Customer Lifetime Value Prediction...")
        clv_model, clv_features = self.customer_lifetime_value_prediction()
        
        logger.info("🎯 Step 2: Customer Segmentation...")
        kmeans_model, cluster_analysis = self.customer_segmentation_clustering()
        
        logger.info("⚠️ Step 3: Churn Prediction...")
        churn_model, churn_features = self.churn_prediction()
        
        logger.info("🛍️ Step 4: Recommendation System...")
        recommendation_data = self.recommendation_system()
        
        logger.info("📊 Step 5: Sales Forecasting...")
        forecast_model = self.sales_forecasting()
        
        logger.info("📋 Step 6: Generate ML Report...")
        self.generate_ml_report()
        
        logger.success("🎉 MACHINE LEARNING ANALYSIS HOÀN THÀNH!")
        logger.info("📊 Kết quả:")
        logger.info("  🤖 5 ML models được tạo")
        logger.info("  📁 Models lưu tại: ./models/")
        logger.info("  📊 Charts tại: ./charts/")
        logger.info("  📋 Report: ./ml_analysis_report.html")
        
        return True

def main():
    """Chạy ML analysis"""
    ml_analyzer = MLAnalyzer()
    ml_analyzer.run_complete_ml_analysis()

if __name__ == "__main__":
    main()