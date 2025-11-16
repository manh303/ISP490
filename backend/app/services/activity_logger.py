from typing import Optional, Dict, Any
from fastapi import Request
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ActivityLogger:
    def __init__(self, db):
        self.db = db
    
    async def log_activity(
        self,
        user_id: Optional[int],
        email: Optional[str],
        action: str,
        resource: Optional[str] = None,
        details: Optional[Dict[Any, Any]] = None,
        request: Optional[Request] = None,
        status: str = "success"
    ):
        """Log user activity"""
        try:
            ip_address = None
            user_agent = None
            
            if request:
                ip_address = request.client.host if request.client else None
                user_agent = request.headers.get("user-agent")
            
            query = """
            INSERT INTO iam.user_activity_logs 
            (user_id, email, action, resource, details, ip_address, user_agent, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """
            
            await self.db.execute_query(query, (
                user_id, email, action, resource,
                json.dumps(details) if details else None,
                ip_address, user_agent, status
            ))
        except Exception as e:
            logger.error(f"Activity logging error: {e}")

    async def get_activity_logs(
        self,
        page: int = 1,
        limit: int = 50,
        user_id: Optional[int] = None,
        action: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ):
        """Get activity logs with filters"""
        try:
            offset = (page - 1) * limit
            
            # Build query with filters
            where_conditions = []
            params = []
            param_count = 0
            
            if user_id:
                param_count += 1
                where_conditions.append(f"user_id = ${param_count}")
                params.append(user_id)
            
            if action:
                param_count += 1
                where_conditions.append(f"action ILIKE ${param_count}")
                params.append(f"%{action}%")
            
            if start_date:
                param_count += 1
                where_conditions.append(f"created_at >= ${param_count}")
                params.append(start_date)
            
            if end_date:
                param_count += 1
                where_conditions.append(f"created_at <= ${param_count}")
                params.append(end_date)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Get logs
            query = f"""
            SELECT log_id, user_id, email, action, resource, details, 
                   ip_address, user_agent, status, created_at
            FROM iam.iam.user_activity_logs
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${param_count + 1} OFFSET ${param_count + 2}
            """
            params.extend([limit, offset])
            
            logs = await self.db.execute_query(query, params)
            
            # Get total count
            count_query = f"SELECT COUNT(*) FROM iam.user_activity_logs {where_clause}"
            count_params = params[:-2] if params else []
            total_result = await self.db.execute_query(count_query, count_params)
            total = total_result[0]['count'] if total_result and len(total_result) > 0 else 0
            
            return {
                "logs": logs,
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": total,
                    "pages": (total + limit - 1) // limit if total > 0 else 0
                }
            }
        except Exception as e:
            logger.error(f"Get activity logs error: {e}")
            raise

    async def get_activity_stats(self, days: int = 7):
        """Get activity statistics"""
        try:
            # Daily activity count
            daily_query = """
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM iam.iam.user_activity_logs
            WHERE created_at >= NOW() - INTERVAL '%s days'
            GROUP BY DATE(created_at)
            ORDER BY date DESC
            """
            daily_stats = await self.db.execute_query(daily_query, (days,))
            
            # Action breakdown
            action_query = """
            SELECT action, COUNT(*) as count
            FROM iam.iam.user_activity_logs
            WHERE created_at >= NOW() - INTERVAL '%s days'
            GROUP BY action
            ORDER BY count DESC
            LIMIT 10
            """
            action_stats = await self.db.execute_query(action_query, (days,))
            
            # Top active users
            user_query = """
            SELECT COALESCE(u.email, l.email) as email, 
                   COALESCE(u.full_name, 'Unknown User') as full_name, 
                   COUNT(l.log_id) as activity_count
            FROM iam.user_activity_logs l
            LEFT JOIN iam.iam_user u ON l.user_id = u.user_id
            WHERE l.created_at >= NOW() - INTERVAL '%s days'
            GROUP BY u.user_id, u.email, u.full_name, l.email
            ORDER BY activity_count DESC
            LIMIT 10
            """
            user_stats = await self.db.execute_query(user_query, (days,))
            
            return {
                "daily_activity": daily_stats,
                "action_breakdown": action_stats,
                "top_users": user_stats
            }
        except Exception as e:
            logger.error(f"Get activity stats error: {e}")
            raise