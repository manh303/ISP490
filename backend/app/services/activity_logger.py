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
        module: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        role_at_time: Optional[str] = None,
        request_method: Optional[str] = None,
        request_path: Optional[str] = None,
        request_payload: Optional[Dict[Any, Any]] = None,
        before_data: Optional[Dict[Any, Any]] = None,
        after_data: Optional[Dict[Any, Any]] = None,
        message: Optional[str] = None,
        details: Optional[Dict[Any, Any]] = None,
        request: Optional[Request] = None,
        status: str = "success"
    ):
        """Log user activity with comprehensive detail tracking"""
        try:
            ip_address = None
            user_agent = None
            
            if request:
                ip_address = request.client.host if request.client else None
                user_agent = request.headers.get("user-agent")
                
                # Auto-extract request details if not provided
                if not request_method:
                    request_method = request.method
                if not request_path:
                    request_path = str(request.url.path)
            
            # Combine resource_type and resource_id for backward compatibility
            resource = f"{resource_type}#{resource_id}" if resource_type and resource_id else None
            
            query = """
            INSERT INTO iam.user_activity_logs 
            (user_id, email, action, module, resource_type, resource, role_at_time,
             request_method, request_payload, before_data, after_data, message,
             details, ip_address, user_agent, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            """
            
            await self.db.execute_query(query, (
                user_id, email, action, module, resource_type, resource, role_at_time,
                request_method,
                json.dumps(request_payload) if request_payload else None,
                json.dumps(before_data) if before_data else None,
                json.dumps(after_data) if after_data else None,
                message,
                json.dumps(details) if details else None,
                ip_address, user_agent, status
            ))
        except Exception as e:
            logger.error(f"Activity logging error: {e}")

    async def get_activity_logs(
        self,
        page: int = 1,
        limit: int = 50,
        sort: str = "-created_at",
        user_id: Optional[int] = None,
        user_email: Optional[str] = None,
        role: Optional[str] = None,
        module: Optional[str] = None,
        action: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        keyword: Optional[str] = None
    ):
        """Get activity logs with comprehensive filters"""
        try:
            offset = (page - 1) * limit
            
            # Build query with filters
            where_conditions = []
            params = []
            param_count = 0
            
            if user_id:
                param_count += 1
                where_conditions.append(f"l.user_id = ${param_count}")
                params.append(user_id)
            
            if user_email:
                param_count += 1
                where_conditions.append(f"l.email ILIKE ${param_count}")
                params.append(f"%{user_email}%")
            
            if role:
                param_count += 1
                where_conditions.append(f"l.role_at_time ILIKE ${param_count}")
                params.append(f"%{role}%")
            
            if module:
                param_count += 1
                where_conditions.append(f"l.module = ${param_count}")
                params.append(module)
            
            if action:
                param_count += 1
                where_conditions.append(f"l.action ILIKE ${param_count}")
                params.append(f"%{action}%")
            
            if status:
                param_count += 1
                where_conditions.append(f"l.status = ${param_count}")
                params.append(status.lower())
            
            if start_date:
                param_count += 1
                where_conditions.append(f"l.created_at >= ${param_count}")
                params.append(start_date)
            
            if end_date:
                param_count += 1
                where_conditions.append(f"l.created_at <= ${param_count}")
                params.append(end_date)
            
            # Keyword search across multiple fields
            if keyword:
                param_count += 1
                where_conditions.append(f"""(
                    l.resource ILIKE ${param_count} OR 
                    l.message ILIKE ${param_count} OR
                    l.action ILIKE ${param_count}
                )""")
                params.append(f"%{keyword}%")
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Parse sort parameter
            sort_direction = "DESC" if sort.startswith("-") else "ASC"
            sort_field = sort.lstrip("-")
            
            # Get logs with user info
            query = f"""
            SELECT l.log_id, l.user_id, l.email, l.action, l.module, l.resource_type,
                   l.resource, l.role_at_time, l.request_method, l.details,
                   l.ip_address, l.user_agent, l.status, l.message, l.created_at,
                   u.full_name
            FROM iam.user_activity_logs l
            LEFT JOIN iam.iam_user u ON l.user_id = u.user_id
            {where_clause}
            ORDER BY l.{sort_field} {sort_direction}
            LIMIT ${param_count + 1} OFFSET ${param_count + 2}
            """
            params.extend([limit, offset])
            
            logs = await self.db.execute_query(query, params)
            
            # Get total count
            count_query = f"SELECT COUNT(*) FROM iam.user_activity_logs l {where_clause}"
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

    async def get_log_detail(self, log_id: int):
        """Get detailed information for a single activity log"""
        try:
            query = """
            SELECT l.log_id, l.user_id, l.email, l.action, l.module, l.resource_type,
                   l.resource, l.role_at_time, l.request_method, l.request_payload,
                   l.before_data, l.after_data, l.message, l.details,
                   l.ip_address, l.user_agent, l.status, l.created_at,
                   u.full_name
            FROM iam.user_activity_logs l
            LEFT JOIN iam.iam_user u ON l.user_id = u.user_id
            WHERE l.log_id = $1
            """
            
            result = await self.db.execute_query(query, (log_id,))
            
            if not result or len(result) == 0:
                return None
            
            log = result[0]
            
            # Parse JSON fields
            return {
                **log,
                'request_payload': json.loads(log['request_payload']) if log.get('request_payload') else None,
                'before_data': json.loads(log['before_data']) if log.get('before_data') else None,
                'after_data': json.loads(log['after_data']) if log.get('after_data') else None,
                'details': json.loads(log['details']) if log.get('details') else None,
            }
        except Exception as e:
            logger.error(f"Get log detail error: {e}")
            raise

    async def get_activity_stats(self, days: int = 7):
        """Get activity statistics"""
        try:
            # Daily activity count
            daily_query = """
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM iam.user_activity_logs
            WHERE created_at >= NOW() - INTERVAL '%s days'
            GROUP BY DATE(created_at)
            ORDER BY date DESC
            """
            daily_stats = await self.db.execute_query(daily_query, (days,))
            
            # Action breakdown
            action_query = """
            SELECT action, COUNT(*) as count
            FROM iam.user_activity_logs
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
    
    async def export_logs(
        self,
        user_id: Optional[int] = None,
        user_email: Optional[str] = None,
        role: Optional[str] = None,
        module: Optional[str] = None,
        action: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        keyword: Optional[str] = None
    ):
        """Export activity logs to CSV format"""
        try:
            import csv
            import io
            
            # Get logs with same filters as get_activity_logs
            result = await self.get_activity_logs(
                page=1,
                limit=10000,  # Export limit
                user_id=user_id,
                user_email=user_email,
                role=role,
                module=module,
                action=action,
                status=status,
                start_date=start_date,
                end_date=end_date,
                keyword=keyword
            )
            
            logs = result['logs']
            
            # Create CSV in memory
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow([
                'Time', 'User Email', 'Full Name', 'Role', 'Action', 'Module',
                'Resource Type', 'Resource', 'Status', 'IP Address', 'Message'
            ])
            
            # Write rows
            for log in logs:
                writer.writerow([
                    log.get('created_at', ''),
                    log.get('email', ''),
                    log.get('full_name', ''),
                    log.get('role_at_time', ''),
                    log.get('action', ''),
                    log.get('module', ''),
                    log.get('resource_type', ''),
                    log.get('resource', ''),
                    log.get('status', ''),
                    log.get('ip_address', ''),
                    log.get('message', '')
                ])
            
            return output.getvalue()
        except Exception as e:
            logger.error(f"Export logs error: {e}")
            raise