import pandas as pd
import numpy as np
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, calinski_harabasz_score
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import seaborn as sns
from loguru import logger
import joblib
from datetime import datetime

class CustomerSegmentation:
    def __init__(self, model_path: str = None):
        self.models = {
            'kmeans': None,
            'dbscan': None
        }
        self.scaler = StandardScaler()
        self.pca = PCA(n_components=2)
        self.best_model = None
        self.best_model_name = None
        self.segment_profiles = None
        self.model_path = model_path or '../data/models'
    
    def create_customer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tạo customer features từ transaction data
        """
        logger.info("Creating customer features...")
        
        # Giả sử chúng ta có customer transaction data
        # Nếu chưa có, tạo synthetic customer data từ product data
        
        if 'customer_id' not in df.columns:
            # Create synthetic customer data
            np.random.seed(42)
            n_customers = min(1000, len(df) // 5)  # 1 customer per 5 products
            
            customer_data = []
            for i in range(n_customers):
                # Random customer behavior
                n_orders = np.random.poisson(5) + 1  # Average 5 orders per customer
                customer_products = df.sample(n=min(n_orders, len(df)))
                
                # Customer metrics
                total_spent = customer_products['price'].sum()
                avg_order_value = total_spent / n_orders
                favorite_category = customer_products['category'].mode().iloc[0] if not customer_products['category'].empty else 'Unknown'
                avg_rating_given = customer_products['rating'].mean()
                days_active = np.random.randint(30, 365)  # Active days
                
                customer_data.append({
                    'customer_id': f'CUST_{i:06d}',
                    'total_orders': n_orders,
                    'total_spent': total_spent,
                    'avg_order_value': avg_order_value,
                    'favorite_category': favorite_category,
                    'avg_rating_given': avg_rating_given,
                    'days_active': days_active,
                    'recency': np.random.randint(1, 90),  # Days since last order
                    'frequency': n_orders / max(days_active, 1) * 30,  # Orders per month
                    'monetary': total_spent
                })
            
            customer_df = pd.DataFrame(customer_data)
        else:
            # Use existing customer data
            customer_df = self._aggregate_customer_data(df)
        
        # RFM Analysis
        customer_df = self._calculate_rfm_scores(customer_df)
        
        return customer_df
    
    def _calculate_rfm_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate RFM (Recency, Frequency, Monetary) scores
        """
        df = df.copy()
        
        # RFM Scores (1-5 scale, 5 is best)
        df['recency_score'] = pd.qcut(df['recency'], q=5, labels=[5,4,3,2,1])  # Lower recency is better
        df['frequency_score'] = pd.qcut(df['frequency'].rank(method='first'), q=5, labels=[1,2,3,4,5])
        df['monetary_score'] = pd.qcut(df['monetary'].rank(method='first'), q=5, labels=[1,2,3,4,5])
        
        # Convert to numeric
        df['recency_score'] = df['recency_score'].astype(int)
        df['frequency_score'] = df['frequency_score'].astype(int)
        df['monetary_score'] = df['monetary_score'].astype(int)
        
        # RFM combined score
        df['rfm_score'] = df['recency_score'] * 100 + df['frequency_score'] * 10 + df['monetary_score']
        
        # Customer value segments based on RFM
        df['customer_segment_rfm'] = df['rfm_score'].apply(self._categorize_rfm)
        
        return df
    
    def _categorize_rfm(self, rfm_score):
        """Categorize customers based on RFM score"""
        if rfm_score >= 444:
            return 'Champions'
        elif rfm_score >= 334:
            return 'Loyal Customers'
        elif rfm_score >= 313:
            return 'Potential Loyalists'
        elif rfm_score >= 155:
            return 'At Risk'
        else:
            return 'Lost'
    
    def train_clustering_models(self, df: pd.DataFrame, n_clusters_range: range = range(3, 8)):
        """
        Train multiple clustering models
        """
        logger.info("Training clustering models...")
        
        # Select features for clustering
        clustering_features = [
            'total_spent', 'avg_order_value', 'total_orders',
            'recency', 'frequency', 'monetary',
            'recency_score', 'frequency_score', 'monetary_score',
            'days_active'
        ]
        
        X = df[clustering_features].copy()
        
        # Handle missing values
        X = X.fillna(X.median())
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Find optimal number of clusters for KMeans
        best_k = self._find_optimal_clusters(X_scaled, n_clusters_range)
        
        # Train KMeans
        kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
        kmeans_labels = kmeans.fit_predict(X_scaled)
        
        # Train DBSCAN
        dbscan = DBSCAN(eps=0.5, min_samples=5)
        dbscan_labels = dbscan.fit_predict(X_scaled)
        
        # Evaluate models
        models_performance = {}
        
        # KMeans evaluation
        if len(set(kmeans_labels)) > 1:
            kmeans_silhouette = silhouette_score(X_scaled, kmeans_labels)
            kmeans_ch = calinski_harabasz_score(X_scaled, kmeans_labels)
            models_performance['kmeans'] = {
                'silhouette_score': kmeans_silhouette,
                'calinski_harabasz_score': kmeans_ch,
                'n_clusters': len(set(kmeans_labels))
            }
        
        # DBSCAN evaluation
        if len(set(dbscan_labels)) > 1:
            dbscan_silhouette = silhouette_score(X_scaled, dbscan_labels)
            dbscan_ch = calinski_harabasz_score(X_scaled, dbscan_labels)
            models_performance['dbscan'] = {
                'silhouette_score': dbscan_silhouette,
                'calinski_harabasz_score': dbscan_ch,
                'n_clusters': len(set(dbscan_labels)),
                'n_noise': list(dbscan_labels).count(-1)
            }
        
        # Store models
        self.models['kmeans'] = kmeans
        self.models['dbscan'] = dbscan
        
        # Select best model based on silhouette score
        if models_performance:
            best_model_name = max(models_performance.keys(), 
                                key=lambda x: models_performance[x]['silhouette_score'])
            self.best_model_name = best_model_name
            self.best_model = self.models[best_model_name]
            
            logger.success(f"Best model: {best_model_name} with silhouette score: {models_performance[best_model_name]['silhouette_score']:.3f}")
        
        # Add cluster labels to dataframe
        if self.best_model_name == 'kmeans':
            df['cluster'] = kmeans_labels
        else:
            df['cluster'] = dbscan_labels
        
        # Create segment profiles
        self.segment_profiles = self._create_segment_profiles(df, clustering_features + ['cluster'])
        
        return models_performance, df
    
    def _find_optimal_clusters(self, X_scaled, n_clusters_range):
        """Find optimal number of clusters using elbow method"""
        inertias = []
        silhouette_scores = []
        
        for k in n_clusters_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            kmeans.fit(X_scaled)
            inertias.append(kmeans.inertia_)
            
            if k > 1:
                sil_score = silhouette_score(X_scaled, kmeans.labels_)
                silhouette_scores.append(sil_score)
            else:
                silhouette_scores.append(0)
        
        # Find elbow point (simplified)
        if len(silhouette_scores) > 2:
            best_k = n_clusters_range[np.argmax(silhouette_scores)]
        else:
            best_k = 4  # Default
        
        logger.info(f"Optimal number of clusters: {best_k}")
        return best_k
    
    def _create_segment_profiles(self, df: pd.DataFrame, feature_columns: list) -> pd.DataFrame:
        """Create customer segment profiles"""
        profiles = df.groupby('cluster').agg({
            'total_spent': ['mean', 'median', 'std'],
            'avg_order_value': ['mean', 'median'],
            'total_orders': ['mean', 'median'],
            'recency': ['mean', 'median'],
            'frequency': ['mean', 'median'],
            'monetary': ['mean', 'median'],
            'customer_id': 'count'
        }).round(2)
        
        # Flatten column names
        profiles.columns = ['_'.join(col).strip() for col in profiles.columns]
        profiles = profiles.rename(columns={'customer_id_count': 'segment_size'})
        
        # Add segment names
        segment_names = {}
        for cluster in profiles.index:
            cluster_data = df[df['cluster'] == cluster]
            avg_monetary = cluster_data['monetary'].mean()
            avg_recency = cluster_data['recency'].mean()
            avg_frequency = cluster_data['frequency'].mean()
            
            # Name segments based on characteristics
            if avg_monetary > df['monetary'].quantile(0.8):
                if avg_recency < df['recency'].quantile(0.3):
                    name = "VIP_Active"
                else:
                    name = "VIP_At_Risk"
            elif avg_frequency > df['frequency'].quantile(0.7):
                name = "Loyal_Regular"
            elif avg_recency > df['recency'].quantile(0.7):
                name = "Lost_Customers"
            else:
                name = f"Regular_Segment_{cluster}"
            
            segment_names[cluster] = name
        
        profiles['segment_name'] = profiles.index.map(segment_names)
        
        logger.info("Customer segment profiles created")
        return profiles
    
    def predict_segment(self, customer_data: pd.DataFrame) -> pd.DataFrame:
        """Predict segment for new customers"""
        if self.best_model is None:
            raise ValueError("Model chưa được train")
        
        clustering_features = [
            'total_spent', 'avg_order_value', 'total_orders',
            'recency', 'frequency', 'monetary',
            'recency_score', 'frequency_score', 'monetary_score',
            'days_active'
        ]
        
        X = customer_data[clustering_features].fillna(customer_data[clustering_features].median())
        X_scaled = self.scaler.transform(X)
        
        predictions = self.best_model.predict(X_scaled)
        customer_data['predicted_cluster'] = predictions
        
        return customer_data
    
    def visualize_segments(self, df: pd.DataFrame, save_path: str = None):
        """Visualize customer segments"""
        clustering_features = [
            'total_spent', 'avg_order_value', 'recency', 'frequency', 'monetary'
        ]
        
        X = df[clustering_features].fillna(df[clustering_features].median())
        X_scaled = self.scaler.transform(X)
        
        # PCA for visualization
        X_pca = self.pca.fit_transform(X_scaled)
        
        plt.figure(figsize=(12, 8))
        
        # Scatter plot
        plt.subplot(2, 2, 1)
        scatter = plt.scatter(X_pca[:, 0], X_pca[:, 1], c=df['cluster'], cmap='viridis', alpha=0.6)
        plt.colorbar(scatter)
        plt.title('Customer Segments (PCA)')
        plt.xlabel('First Principal Component')
        plt.ylabel('Second Principal Component')
        
        # Segment size distribution
        plt.subplot(2, 2, 2)
        segment_counts = df['cluster'].value_counts().sort_index()
        plt.bar(segment_counts.index, segment_counts.values)
        plt.title('Segment Size Distribution')
        plt.xlabel('Cluster')
        plt.ylabel('Number of Customers')
        
        # RFM distribution by segment
        plt.subplot(2, 2, 3)
        sns.boxplot(data=df, x='cluster', y='monetary')
        plt.title('Monetary Value by Segment')
        plt.xlabel('Cluster')
        plt.ylabel('Monetary Value')
        
        plt.subplot(2, 2, 4)
        sns.boxplot(data=df, x='cluster', y='frequency')
        plt.title('Frequency by Segment')
        plt.xlabel('Cluster')
        plt.ylabel('Frequency')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Visualization saved to: {save_path}")
        
        plt.show()
    
    def save_model(self, filename: str = None):
        """Save clustering model"""
        if not filename:
            filename = f"customer_segmentation_{self.best_model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
        
        model_data = {
            'model': self.best_model,
            'scaler': self.scaler,
            'pca': self.pca,
            'model_name': self.best_model_name,
            'segment_profiles': self.segment_profiles
        }
        
        save_path = f"{self.model_path}/{filename}"
        joblib.dump(model_data, save_path)
        logger.info(f"Model saved to: {save_path}")
        return save_path