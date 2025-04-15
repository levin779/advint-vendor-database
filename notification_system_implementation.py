"""
Notification System Implementation for Advint Pharma Vendor Database System

This script implements the notification system for the Advint Pharma
Vendor Database system, providing alerts for regulatory approvals and data conflicts.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import threading
import queue
import requests
from typing import List, Dict, Any, Optional
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text, JSON, Float

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("notification_system.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection settings
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING", "postgresql://postgres:postgres@localhost:5432/advint_vendor_db")

# Email settings
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "notifications@advintpharma.in")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "your_password_here")
EMAIL_FROM = os.getenv("EMAIL_FROM", "notifications@advintpharma.in")

# API settings
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000/api")
API_TIMEOUT = 30  # seconds

# Notification settings
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))  # 5 minutes in seconds
MAX_RETRIES = 3
RETRY_DELAY = 60  # seconds

# Define Base for SQLAlchemy models
Base = declarative_base()

#######################
# Database Models
#######################

class User(Base):
    """User model for notification recipients"""
    __tablename__ = "users"
    
    user_id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    first_name = Column(String(50))
    last_name = Column(String(50))
    role = Column(String(20), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    notification_settings = relationship("NotificationSetting", back_populates="user")
    notifications = relationship("Notification", back_populates="user")


class NotificationSetting(Base):
    """Notification settings model"""
    __tablename__ = "notification_settings"
    
    setting_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    notification_type = Column(String(50), nullable=False)
    is_enabled = Column(Boolean, default=True)
    delivery_method = Column(String(20), default="in_app")  # in_app, email, both
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="notification_settings")


class Notification(Base):
    """Notification model"""
    __tablename__ = "notifications"
    
    notification_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    notification_type = Column(String(50), nullable=False)
    message = Column(Text, nullable=False)
    entity_type = Column(String(50))  # vendor, product, approval, etc.
    entity_id = Column(Integer)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="notifications")


class RegulatoryApproval(Base):
    """Regulatory approval model"""
    __tablename__ = "regulatory_approvals"
    
    approval_id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer, nullable=False)
    product_id = Column(Integer, nullable=True)
    approval_type = Column(String(50), nullable=False)
    regulatory_body = Column(String(50), nullable=False)
    approval_number = Column(String(100))
    issue_date = Column(DateTime)
    expiry_date = Column(DateTime)
    status = Column(String(20), default="active")
    last_checked = Column(DateTime, default=datetime.utcnow)
    last_notification_sent = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DataConflict(Base):
    """Data conflict model"""
    __tablename__ = "data_conflicts"
    
    conflict_id = Column(Integer, primary_key=True)
    entity_type = Column(String(50), nullable=False)  # vendor, product, approval, etc.
    entity_id = Column(Integer, nullable=False)
    field_name = Column(String(100), nullable=False)
    source_1 = Column(String(100), nullable=False)
    value_1 = Column(Text, nullable=False)
    source_2 = Column(String(100), nullable=False)
    value_2 = Column(Text, nullable=False)
    conflict_status = Column(String(20), default="unresolved")  # unresolved, resolved, ignored
    resolution = Column(Text)
    resolved_by = Column(Integer, ForeignKey("users.user_id"))
    resolved_at = Column(DateTime)
    last_notification_sent = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class NotificationQueue(Base):
    """Queue for notifications to be processed"""
    __tablename__ = "notification_queue"
    
    queue_id = Column(Integer, primary_key=True)
    notification_type = Column(String(50), nullable=False)
    entity_type = Column(String(50))
    entity_id = Column(Integer)
    message = Column(Text, nullable=False)
    recipients = Column(JSON)  # List of user_ids or roles
    priority = Column(Integer, default=1)  # 1=low, 2=medium, 3=high
    status = Column(String(20), default="pending")  # pending, processing, sent, failed
    retry_count = Column(Integer, default=0)
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


#######################
# Database Connection
#######################

def get_db_session():
    """Create and return a database session"""
    engine = create_engine(DB_CONNECTION_STRING)
    Session = sessionmaker(bind=engine)
    return Session()


def init_db():
    """Initialize database tables"""
    engine = create_engine(DB_CONNECTION_STRING)
    Base.metadata.create_all(engine)
    logger.info("Database tables created")


#######################
# Notification Handlers
#######################

class NotificationSystem:
    """Main notification system class"""
    
    def __init__(self):
        self.db_session = get_db_session()
        self.notification_queue = queue.PriorityQueue()
        self.running = False
        self.worker_thread = None
    
    def start(self):
        """Start the notification system"""
        if self.running:
            logger.warning("Notification system is already running")
            return
        
        self.running = True
        self.worker_thread = threading.Thread(target=self._worker_loop)
        self.worker_thread.daemon = True
        self.worker_thread.start()
        
        # Start regulatory approval checker
        approval_checker = threading.Thread(target=self._check_regulatory_approvals)
        approval_checker.daemon = True
        approval_checker.start()
        
        # Start data conflict checker
        conflict_checker = threading.Thread(target=self._check_data_conflicts)
        conflict_checker.daemon = True
        conflict_checker.start()
        
        logger.info("Notification system started")
    
    def stop(self):
        """Stop the notification system"""
        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5.0)
        self.db_session.close()
        logger.info("Notification system stopped")
    
    def _worker_loop(self):
        """Worker loop to process notification queue"""
        while self.running:
            try:
                # Get next notification from queue
                try:
                    priority, queue_item = self.notification_queue.get(timeout=1.0)
                    queue_id = queue_item.get("queue_id")
                    
                    # Update status to processing
                    queue_record = self.db_session.query(NotificationQueue).filter_by(queue_id=queue_id).first()
                    if queue_record:
                        queue_record.status = "processing"
                        self.db_session.commit()
                    
                    # Process notification
                    success = self._process_notification(queue_item)
                    
                    # Update status
                    if queue_record:
                        if success:
                            queue_record.status = "sent"
                        else:
                            queue_record.retry_count += 1
                            if queue_record.retry_count >= MAX_RETRIES:
                                queue_record.status = "failed"
                                queue_record.error_message = "Max retries exceeded"
                            else:
                                queue_record.status = "pending"
                                # Re-queue with delay
                                time.sleep(RETRY_DELAY)
                                self.notification_queue.put((priority, queue_item))
                        
                        self.db_session.commit()
                    
                    # Mark task as done
                    self.notification_queue.task_done()
                
                except queue.Empty:
                    # No items in queue, check database for pending notifications
                    self._load_pending_notifications()
                    time.sleep(1.0)
            
            except Exception as e:
                logger.error(f"Error in worker loop: {str(e)}")
                time.sleep(5.0)
    
    def _load_pending_notifications(self):
        """Load pending notifications from database into queue"""
        try:
            # Get pending notifications
            pending_notifications = self.db_session.query(NotificationQueue).filter_by(
                status="pending"
            ).order_by(
                NotificationQueue.priority.desc(),
                NotificationQueue.created_at.asc()
            ).limit(100).all()
            
            # Add to queue
            for notification in pending_notifications:
                queue_item = {
                    "queue_id": notification.queue_id,
                    "notification_type": notification.notification_type,
                    "entity_type": notification.entity_type,
                    "entity_id": notification.entity_id,
                    "message": notification.message,
                    "recipients": notification.recipients,
                    "priority": notification.priority
                }
                
                self.notification_queue.put((4 - notification.priority, queue_item))  # Invert priority for queue
        
        except Exception as e:
            logger.error(f"Error loading pending notifications: {str(e)}")
    
    def _process_notification(self, queue_item):
        """Process a notification"""
        try:
            notification_type = queue_item.get("notification_type")
            recipients = queue_item.get("recipients", [])
            message = queue_item.get("message")
            entity_type = queue_item.get("entity_type")
            entity_id = queue_item.get("entity_id")
            
            # Get recipient users
            users = []
            if isinstance(recipients, list):
                # Direct user IDs
                if all(isinstance(r, int) for r in recipients):
                    users = self.db_session.query(User).filter(User.user_id.in_(recipients)).all()
                # Roles
                elif all(isinstance(r, str) for r in recipients):
                    users = self.db_session.query(User).filter(User.role.in_(recipients)).all()
            
            # Send notifications to each user
            for user in users:
                # Check notification settings
                setting = self.db_session.query(NotificationSetting).filter_by(
                    user_id=user.user_id,
                    notification_type=notification_type
                ).first()
                
                # Skip if notifications are disabled for this type
                if setting and not setting.is_enabled:
                    continue
                
                # Determine delivery method
                delivery_method = "in_app"
                if setting:
                    delivery_method = setting.delivery_method
                
                # Create in-app notification
                if delivery_method in ["in_app", "both"]:
                    notification = Notification(
                        user_id=user.user_id,
                        notification_type=notification_type,
                        message=message,
                        entity_type=entity_type,
                        entity_id=entity_id
                    )
                    self.db_session.add(notification)
                
                # Send email notification
                if delivery_method in ["email", "both"]:
                    self._send_email_notification(user, notification_type, message, entity_type, entity_id)
            
            # Commit changes
            self.db_session.commit()
            return True
        
        except Exception as e:
            logger.error(f"Error processing notification: {str(e)}")
            return False
    
    def _send_email_notification(self, user, notification_type, message, entity_type, entity_id):
        """Send email notification"""
        try:
            # Create email
            email = MIMEMultipart()
            email["From"] = EMAIL_FROM
            email["To"] = user.email
            email["Subject"] = f"Advint Pharma Notification: {notification_type}"
            
            # Email body
            body = f"""
            <html>
            <body>
                <h2>Advint Pharma Vendor Database Notification</h2>
                <p><strong>Type:</strong> {notification_type}</p>
                <p><strong>Message:</strong> {message}</p>
                <p>Please log in to the Vendor Database system for more details.</p>
                <p>This is an automated message, please do not reply.</p>
            </body>
            </html>
            """
            
            email.attach(MIMEText(body, "html"))
            
            # Send email
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(email)
            
            logger.info(f"Email notification sent to {user.email}")
            return True
        
        except Exception as e:
            logger.error(f"Error sending email notification: {str(e)}")
            return False
    
    def add_notification(self, notification_type, message, entity_type=None, entity_id=None, recipients=None, priority=1):
        """Add a notification to the queue"""
        try:
            # Create notification queue record
            queue_record = NotificationQueue(
                notification_type=notification_type,
                entity_type=entity_type,
                entity_id=entity_id,
                message=message,
                recipients=recipients,
                priority=priority,
                status="pending"
            )
            
            self.db_session.add(queue_record)
            self.db_session.commit()
            
            # Add to in-memory queue
            queue_item = {
                "queue_id": queue_record.queue_id,
                "notification_type": notification_type,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "message": message,
                "recipients": recipients,
                "priority": priority
            }
            
            self.notification_queue.put((4 - priority, queue_item))  # Invert priority for queue
            
            logger.info(f"Notification added to queue: {notification_type}")
            return True
        
        except Exception as e:
            logger.error(f"Error adding notification to queue: {str(e)}")
            return False
    
    def _check_regulatory_approvals(self):
        """Check for regulatory approval updates"""
        while self.running:
            try:
                # Get approvals that need checking
                check_date = datetime.utcnow() - timedelta(days=30)  # Check approvals updated in the last 30 days
                approvals = self.db_session.query(RegulatoryApproval).filter(
                    RegulatoryApproval.updated_at >= check_date,
                    RegulatoryApproval.last_notification_sent.is_(None) | 
                    (RegulatoryApproval.last_notification_sent < RegulatoryApproval.updated_at)
                ).all()
                
                for approval in approvals:
                    # Get vendor and product info
                    vendor_name = self._get_vendor_name(approval.vendor_id)
                    product_name = self._get_product_name(approval.product_id) if approval.product_id else "N/A"
                    
                    # Create notification message
                    message = f"Regulatory approval update: {approval.approval_type} from {approval.regulatory_body} for {vendor_name} ({product_name}). Status: {approval.status}."
                    
                    # Add notification
                    self.add_notification(
                        notification_type="regulatory_approval",
                        message=message,
                        entity_type="approval",
                        entity_id=approval.approval_id,
                        recipients=["admin", "compliance_manager"],  # Roles that should receive this notification
                        priority=2  # Medium priority
                    )
                    
                    # Update last notification sent
                    approval.last_notification_sent = datetime.utcnow()
                
                # Commit changes
                self.db_session.commit()
                
                # Sleep before next check
                time.sleep(CHECK_INTERVAL)
            
            except Exception as e:
                logger.error(f"Error checking regulatory approvals: {str(e)}")
                time.sleep(CHECK_INTERVAL)
    
    def _check_data_conflicts(self):
        """Check for data conflicts"""
        while self.running:
            try:
                # Get unresolved conflicts that need notification
                conflicts = self.db_session.query(DataConflict).filter(
                    DataConflict.conflict_status == "unresolved",
                    DataConflict.last_notification_sent.is_(None) | 
                    (DataConflict.last_notification_sent < DataConflict.updated_at)
                ).all()
                
                for conflict in conflicts:
                    # Get entity info
                    entity_name = self._get_entity_name(conflict.entity_type, conflict.entity_id)
                    
                    # Create notification message
                    message = f"Data conflict detected for {conflict.entity_type} '{entity_name}' in field '{conflict.field_name}'. Values: '{conflict.value_1}' (from {conflict.source_1}) vs '{conflict.value_2}' (from {conflict.source_2})."
                    
                    # Add notification
                    self.add_notification(
                        notification_type="data_conflict",
                        message=message,
                        entity_type=conflict.entity_type,
                        entity_id=conflict.entity_id,
                        recipients=["admin", "data_manager"],  # Roles that should receive this notification
                        priority=2  # Medium priority
                    )
                    
                    # Update last notification sent
                    conflict.last_notification_sent = datetime.utcnow()
                
                # Commit changes
                self.db_session.commit()
                
                # Sleep before next check
                time.sleep(CHECK_INTERVAL)
            
            except Exception as e:
                logger.error(f"Error checking data conflicts: {str(e)}")
                time.sleep(CHECK_INTERVAL)
    
    def _get_vendor_name(self, vendor_id):
        """Get vendor name from API"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/vendors/{vendor_id}",
                timeout=API_TIMEOUT
            )
            response.raise_for_status()
            
            vendor_data = response.json()
            return vendor_data.get("company_name", f"Vendor {vendor_id}")
        
        except Exception as e:
            logger.error(f"Error getting vendor name: {str(e)}")
            return f"Vendor {vendor_id}"
    
    def _get_product_name(self, product_id):
        """Get product name from API"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/products/{product_id}",
                timeout=API_TIMEOUT
            )
            response.raise_for_status()
            
            product_data = response.json()
            return product_data.get("chemical_name", f"Product {product_id}")
        
        except Exception as e:
            logger.error(f"Error getting product name: {str(e)}")
            return f"Product {product_id}"
    
    def _get_entity_name(self, entity_type, entity_id):
        """Get entity name based on type"""
        if entity_type == "vendor":
            return self._get_vendor_name(entity_id)
        elif entity_type == "product":
            return self._get_product_name(entity_id)
        else:
            return f"{entity_type} {entity_id}"


#######################
# API Endpoints
#######################

class NotificationAPI:
    """API for notification system"""
    
    def __init__(self, notification_system):
        self.notification_system = notification_system
    
    def get_notifications(self, user_id, is_read=None, limit=100):
        """Get notifications for a user"""
        try:
            query = self.notification_system.db_session.query(Notification).filter_by(user_id=user_id)
            
            if is_read is not None:
                query = query.filter_by(is_read=is_read)
            
            notifications = query.order_by(Notification.created_at.desc()).limit(limit).all()
            
            return [
                {
                    "notification_id": n.notification_id,
                    "notification_type": n.notification_type,
                    "message": n.message,
                    "entity_type": n.entity_type,
                    "entity_id": n.entity_id,
                    "is_read": n.is_read,
                    "created_at": n.created_at.isoformat()
                }
                for n in notifications
            ]
        
        except Exception as e:
            logger.error(f"Error getting notifications: {str(e)}")
            return []
    
    def mark_notification_read(self, notification_id):
        """Mark a notification as read"""
        try:
            notification = self.notification_system.db_session.query(Notification).filter_by(
                notification_id=notification_id
            ).first()
            
            if notification:
                notification.is_read = True
                self.notification_system.db_session.commit()
                return True
            
            return False
        
        except Exception as e:
            logger.error(f"Error marking notification as read: {str(e)}")
            return False
    
    def get_notification_settings(self, user_id):
        """Get notification settings for a user"""
        try:
            settings = self.notification_system.db_session.query(NotificationSetting).filter_by(
                user_id=user_id
            ).all()
            
            return [
                {
                    "setting_id": s.setting_id,
                    "notification_type": s.notification_type,
                    "is_enabled": s.is_enabled,
                    "delivery_method": s.delivery_method
                }
                for s in settings
            ]
        
        except Exception as e:
            logger.error(f"Error getting notification settings: {str(e)}")
            return []
    
    def update_notification_settings(self, user_id, settings):
        """Update notification settings for a user"""
        try:
            for setting in settings:
                notification_type = setting.get("notification_type")
                is_enabled = setting.get("is_enabled")
                delivery_method = setting.get("delivery_method")
                
                # Get existing setting
                existing_setting = self.notification_system.db_session.query(NotificationSetting).filter_by(
                    user_id=user_id,
                    notification_type=notification_type
                ).first()
                
                if existing_setting:
                    # Update existing setting
                    existing_setting.is_enabled = is_enabled
                    existing_setting.delivery_method = delivery_method
                else:
                    # Create new setting
                    new_setting = NotificationSetting(
                        user_id=user_id,
                        notification_type=notification_type,
                        is_enabled=is_enabled,
                        delivery_method=delivery_method
                    )
                    self.notification_system.db_session.add(new_setting)
            
            # Commit changes
            self.notification_system.db_session.commit()
            return True
        
        except Exception as e:
            logger.error(f"Error updating notification settings: {str(e)}")
            return False
    
    def add_manual_notification(self, notification_type, message, entity_type=None, entity_id=None, recipients=None, priority=1):
        """Add a manual notification"""
        return self.notification_system.add_notification(
            notification_type=notification_type,
            message=message,
            entity_type=entity_type,
            entity_id=entity_id,
            recipients=recipients,
            priority=priority
        )


#######################
# Main Application
#######################

def main():
    """Main application entry point"""
    try:
        # Initialize database
        init_db()
        
        # Create notification system
        notification_system = NotificationSystem()
        
        # Create API
        notification_api = NotificationAPI(notification_system)
        
        # Start notification system
        notification_system.start()
        
        # Keep running
        logger.info("Notification system running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Stopping notification system...")
        notification_system.stop()
        logger.info("Notification system stopped")
    
    except Exception as e:
        logger.error(f"Error in main application: {str(e)}")


if __name__ == "__main__":
    main()
