import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.decomposition import NMF
from sklearn.neighbors import NearestNeighbors
import joblib
from loguru import logger
from datetime import datetime

class ProductRecommender:
    def __init__(self, model_path: str = None):
        self.content_model = None  # Content-based filtering
        self.collaborative_model = None  # Collaborative filtering
        self.tfidf_vectorizer = TfidfVectorizer(max_features=5000, stop_words='english')
        self.item_similarity_matrix = None
        self.user_item_matrix = None
        self.model_path = model_path or '../data/models'
    
    def prepare_recommendation_data(self, df: pd.DataFrame) -> dict:
        """
        Prepare data cho recommendation system
        """
        logger.info("Preparing recommendation data...")
        
        # Create synthetic user-item interactions
        np.random.seed(42)
        n_users = min(500, len(df) // 10)
        
        interactions = []
        for user_id in range(n_users):
            # Random number of interactions per user
            n_interactions = np.random.poisson(8) + 1
            user_products = df.sample(n=min(n_interactions, len(df)))
            
            for _, product in user_products.iterrows():
                # Create implicit feedback (views, purchases)
                rating = np.random.choice([3, 4, 5], p=[0.2, 0.3, 0.5])  # Positive bias
                interactions.append({
                    'user_id': f'USER_{user_id:06d}',
                    'product_id': product['product_id'],
                    'rating': rating,
                    'category': product['category'],
                    'price': product['price']
                })
        
        interactions_df = pd.DataFrame(interactions)
        
        return {
            'products': df,
            'interactions': interactions_df,
            'user_item_matrix': self._create_user_item_matrix(interactions_df)
        }
    
    def _create_user_item_matrix(self, interactions_df: pd.DataFrame) -> pd.DataFrame:
        """Create user-item interaction matrix"""
        user_item_matrix = interactions_df.pivot_table(
            index='user_id', 
            columns='product_id', 
            values='rating', 
            fill_value=0
        )
        
        logger.info(f"User-item matrix shape: {user_item_matrix.shape}")
        return user_item_matrix
    
    def build_content_based_model(self, products_df: pd.DataFrame):
        """
        Build content-based recommendation model
        """
        logger.info("Building content-based model...")
        
        # Combine text features
        products_df['combined_features'] = (
            products_df['name'].fillna('') + ' ' + 
            products_df['category'].fillna('') + ' ' + 
            products_df['brand'].fillna('')
        )
        
        # Create TF-IDF matrix
        tfidf_matrix = self.tfidf_vectorizer.fit_transform(products_df['combined_features'])
        
        # Calculate item similarity
        self.item_similarity_matrix = cosine_similarity(tfidf_matrix)
        
        logger.info(f"Content-based model built. Similarity matrix shape: {self.item_similarity_matrix.shape}")
    
    def build_collaborative_model(self, user_item_matrix: pd.DataFrame, n_components: int = 50):
        """
        Build collaborative filtering model using Matrix Factorization
        """
        logger.info("Building collaborative filtering model...")
        
        self.user_item_matrix = user_item_matrix
        
        # Use Non-negative Matrix Factorization
        self.collaborative_model = NMF(n_components=n_components, random_state=42)
        
        # Fit model
        W = self.collaborative_model.fit_transform(user_item_matrix.values)
        H = self.collaborative_model.components_
        
        # Reconstruct user-item matrix
        self.reconstructed_matrix = np.dot(W, H)
        
        logger.info(f"Collaborative model built with {n_components} components")
    
    def get_content_based_recommendations(self, product_id: str, products_df: pd.DataFrame, 
                                        n_recommendations: int = 10):
        """
        Get content-based recommendations cho 1 product
        """
        if self.item_similarity_matrix is None:
            raise ValueError("Content-based model chưa được build")
        
        try:
            # Find product index
            product_index = products_df[products_df['product_id'] == product_id].index[0]
            
            # Get similarity scores
            similarity_scores = self.item_similarity_matrix[product_index]
            
            # Get top similar products
            similar_indices = similarity_scores.argsort()[::-1][1:n_recommendations+1]  # Exclude self
            
            recommendations = products_df.iloc[similar_indices][
                ['product_id', 'name', 'category', 'price', 'rating']
            ].copy()
            
            recommendations['similarity_score'] = similarity_scores[similar_indices]
            
            return recommendations
        
        except IndexError:
            logger.warning(f"Product {product_id} not found")
            return pd.DataFrame()
    
    def get_collaborative_recommendations(self, user_id: str, n_recommendations: int = 10):
        """
        Get collaborative filtering recommendations cho user
        """
        if self.collaborative_model is None or self.user_item_matrix is None:
            raise ValueError("Collaborative model chưa được build")
        
        try:
            # Find user index
            user_index = self.user_item_matrix.index.get_loc(user_id)
            
            # Get user's predicted ratings
            user_predictions = self.reconstructed_matrix[user_index]
            
            # Get items user hasn't interacted with
            user_interactions = self.user_item_matrix.iloc[user_index]
            uninteracted_items = user_interactions[user_interactions == 0].index
            
            # Get predictions for uninteracted items
            item_indices = [self.user_item_matrix.columns.get_loc(item) for item in uninteracted_items]
            item_predictions = user_predictions[item_indices]
            
            # Get top recommendations
            top_indices = item_predictions.argsort()[::-1][:n_recommendations]
            recommended_items = uninteracted_items[top_indices]
            predicted_ratings = item_predictions[top_indices]
            
            recommendations = pd.DataFrame({
                'product_id': recommended_items,
                'predicted_rating': predicted_ratings
            })
            
            return recommendations
        
        except KeyError:
            logger.warning(f"User {user_id} not found")
            return pd.DataFrame()
    
    def get_hybrid_recommendations(self, user_id: str, products_df: pd.DataFrame, 
                                 n_recommendations: int = 10, content_weight: float = 0.3):
        """
        Hybrid recommendation combining content-based và collaborative filtering
        """
        # Get user's interaction history
        try:
            user_items = self.user_item_matrix.loc[user_id]
            recent_items = user_items[user_items > 0].index[-5:]  # Last 5 items
        except KeyError:
            # New user - use popular items
            recent_items = products_df.nlargest(5, 'rating')['product_id'].tolist()
        
        all_recommendations = []
        
        # Content-based recommendations từ recent items
        for item_id in recent_items:
            content_recs = self.get_content_based_recommendations(
                item_id, products_df, n_recommendations=5
            )
            if not content_recs.empty:
                content_recs['rec_type'] = 'content'
                content_recs['weight'] = content_weight
                all_recommendations.append(content_recs)
        
        # Collaborative filtering recommendations
        collab_recs = self.get_collaborative_recommendations(user_id, n_recommendations)
        if not collab_recs.empty:
            # Merge với product info
            collab_recs = collab_recs.merge(
                products_df[['product_id', 'name', 'category', 'price', 'rating']], 
                on='product_id', how='left'
            )
            collab_recs['rec_type'] = 'collaborative'
            collab_recs['weight'] = 1 - content_weight
            collab_recs = collab_recs.rename(columns={'predicted_rating': 'similarity_score'})
            all_recommendations.append(collab_recs)
        
        if not all_recommendations:
            # Fallback to popular items
            return products_df.nlargest(n_recommendations, 'rating')[
                ['product_id', 'name', 'category', 'price', 'rating']
            ]
        
        # Combine recommendations
        combined_recs = pd.concat(all_recommendations, ignore_index=True)
        
        # Calculate hybrid score
        combined_recs['hybrid_score'] = combined_recs['similarity_score'] * combined_recs['weight']
        
        # Remove duplicates và get top recommendations
        final_recs = combined_recs.groupby('product_id').agg({
            'hybrid_score': 'sum',
            'name': 'first',
            'category': 'first',
            'price': 'first',
            'rating': 'first'
        }).reset_index()
        
        final_recs = final_recs.nlargest(n_recommendations, 'hybrid_score')
        
        return final_recs
    
    def evaluate_recommendations(self, test_interactions: pd.DataFrame, n_recommendations: int = 10):
        """
        Evaluate recommendation quality
        """
        logger.info("Evaluating recommendations...")
        
        precision_scores = []
        recall_scores = []
        
        test_users = test_interactions['user_id'].unique()[:100]  # Sample for evaluation
        
        for user_id in test_users:
            # Get actual items user interacted with
            actual_items = set(test_interactions[test_interactions['user_id'] == user_id]['product_id'])
            
            if len(actual_items) == 0:
                continue
            
            # Get recommendations
            try:
                recommendations = self.get_collaborative_recommendations(user_id, n_recommendations)
                if recommendations.empty:
                    continue
                    
                recommended_items = set(recommendations['product_id'])
                
                # Calculate precision và recall
                intersection = actual_items.intersection(recommended_items)
                
                precision = len(intersection) / len(recommended_items) if recommended_items else 0
                recall = len(intersection) / len(actual_items) if actual_items else 0
                
                precision_scores.append(precision)
                recall_scores.append(recall)
                
            except Exception as e:
                continue
        
        avg_precision = np.mean(precision_scores) if precision_scores else 0
        avg_recall = np.mean(recall_scores) if recall_scores else 0
        f1_score = 2 * (avg_precision * avg_recall) / (avg_precision + avg_recall) if (avg_precision + avg_recall) > 0 else 0
        
        logger.info(f"Recommendation Metrics:")
        logger.info(f"Precision: {avg_precision:.3f}")
        logger.info(f"Recall: {avg_recall:.3f}")
        logger.info(f"F1-Score: {f1_score:.3f}")
        
        return {
            'precision': avg_precision,
            'recall': avg_recall,
            'f1_score': f1_score
        }
    
    def save_model(self, filename: str = None):
        """Save recommendation models"""
        if not filename:
            filename = f"product_recommender_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
        
        model_data = {
            'collaborative_model': self.collaborative_model,
            'tfidf_vectorizer': self.tfidf_vectorizer,
            'item_similarity_matrix': self.item_similarity_matrix,
            'user_item_matrix': self.user_item_matrix,
            'reconstructed_matrix': self.reconstructed_matrix
        }
        
        save_path = f"{self.model_path}/{filename}"
        joblib.dump(model_data, save_path)
        logger.info(f"Recommendation model saved to: {save_path}")
        return save_path