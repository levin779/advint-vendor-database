"""
Database Implementation for Advint Pharma Vendor Database System

This script implements the database schema and API layer for the Advint Pharma
Vendor Database system based on the previously designed architecture.
"""

import os
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

# Database libraries
import psycopg2
from psycopg2.extras import Json, DictCursor
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSONB

# API Framework
from fastapi import FastAPI, Depends, HTTPException, status, Query, Path, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, Field, EmailStr
import uvicorn

# Utility libraries
import jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("database_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection settings
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "advint_vendor_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# JWT Settings
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-for-jwt")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Database connection
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# FastAPI app
app = FastAPI(
    title="Advint Pharma Vendor Database API",
    description="API for accessing and managing pharmaceutical vendor information",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# OAuth2 setup
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

#######################
# Database Models
#######################

class Vendor(Base):
    """Vendor/Company model"""
    __tablename__ = "vendors"
    
    vendor_id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String(255), nullable=False, index=True)
    address = Column(Text)
    city = Column(String(100), index=True)
    state_province = Column(String(100))
    country = Column(String(100), index=True)
    postal_code = Column(String(20))
    phone = Column(String(50))
    email = Column(String(255))
    website = Column(String(255))
    year_established = Column(Integer)
    company_size = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_quality_score = Column(Float)
    last_verified_date = Column(DateTime)
    
    # Relationships
    contacts = relationship("VendorContact", back_populates="vendor")
    products = relationship("VendorProduct", back_populates="vendor")
    certifications = relationship("VendorCertification", back_populates="vendor")
    regulatory_approvals = relationship("RegulatoryApproval", back_populates="vendor")


class VendorContact(Base):
    """Vendor contact information model"""
    __tablename__ = "vendor_contacts"
    
    contact_id = Column(Integer, primary_key=True, index=True)
    vendor_id = Column(Integer, ForeignKey("vendors.vendor_id"))
    contact_name = Column(String(255))
    position = Column(String(100))
    department = Column(String(100))
    phone = Column(String(50))
    email = Column(String(255))
    is_primary = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    vendor = relationship("Vendor", back_populates="contacts")


class Product(Base):
    """Product model"""
    __tablename__ = "products"
    
    product_id = Column(Integer, primary_key=True, index=True)
    cas_number = Column(String(50), unique=True, index=True)
    chemical_name = Column(String(255), nullable=False, index=True)
    common_name = Column(String(255), index=True)
    molecular_formula = Column(String(100))
    molecular_weight = Column(Float)
    product_category = Column(String(100))
    therapeutic_category = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    structure_data = Column(JSONB)
    
    # Relationships
    synonyms = relationship("ProductSynonym", back_populates="product")
    vendors = relationship("VendorProduct", back_populates="product")
    regulatory_approvals = relationship("RegulatoryApproval", back_populates="product")


class ProductSynonym(Base):
    """Product synonym model"""
    __tablename__ = "product_synonyms"
    
    synonym_id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.product_id"))
    synonym_name = Column(String(255), nullable=False, index=True)
    synonym_type = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    product = relationship("Product", back_populates="synonyms")


class VendorProduct(Base):
    """Vendor-Product relationship model"""
    __tablename__ = "vendor_products"
    
    vendor_product_id = Column(Integer, primary_key=True, index=True)
    vendor_id = Column(Integer, ForeignKey("vendors.vendor_id"), index=True)
    product_id = Column(Integer, ForeignKey("products.product_id"), index=True)
    min_order_quantity = Column(String(100))
    capacity = Column(String(100))
    lead_time = Column(String(50))
    product_grade = Column(String(100))
    pricing_info = Column(JSONB)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    vendor = relationship("Vendor", back_populates="products")
    product = relationship("Product", back_populates="vendors")


class Certification(Base):
    """Certification type model"""
    __tablename__ = "certifications"
    
    certification_id = Column(Integer, primary_key=True, index=True)
    certification_name = Column(String(100), nullable=False, index=True)
    issuing_body = Column(String(255))
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    vendor_certifications = relationship("VendorCertification", back_populates="certification")


class VendorCertification(Base):
    """Vendor-Certification relationship model"""
    __tablename__ = "vendor_certifications"
    
    vendor_certification_id = Column(Integer, primary_key=True, index=True)
    vendor_id = Column(Integer, ForeignKey("vendors.vendor_id"), index=True)
    certification_id = Column(Integer, ForeignKey("certifications.certification_id"), index=True)
    certificate_number = Column(String(100))
    issue_date = Column(DateTime)
    expiry_date = Column(DateTime)
    status = Column(String(50), index=True)
    document_url = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    vendor = relationship("Vendor", back_populates="certifications")
    certification = relationship("Certification", back_populates="vendor_certifications")


class RegulatoryApproval(Base):
    """Regulatory approval model"""
    __tablename__ = "regulatory_approvals"
    
    approval_id = Column(Integer, primary_key=True, index=True)
    vendor_id = Column(Integer, ForeignKey("vendors.vendor_id"), index=True)
    product_id = Column(Integer, ForeignKey("products.product_id"), index=True)
    approval_type = Column(String(50), nullable=False, index=True)
    regulatory_body = Column(String(100), nullable=False, index=True)
    approval_number = Column(String(100))
    issue_date = Column(DateTime)
    expiry_date = Column(DateTime)
    status = Column(String(50), index=True)
    document_url = Column(String(255))
    additional_info = Column(JSONB)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    vendor = relationship("Vendor", back_populates="regulatory_approvals")
    product = relationship("Product", back_populates="regulatory_approvals")


class DataSource(Base):
    """Data source model"""
    __tablename__ = "data_sources"
    
    source_id = Column(Integer, primary_key=True, index=True)
    source_name = Column(String(100), nullable=False)
    source_type = Column(String(50), nullable=False)
    source_url = Column(String(255))
    api_endpoint = Column(String(255))
    auth_method = Column(String(50))
    credentials = Column(JSONB)
    last_sync_time = Column(DateTime)
    sync_frequency = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    mappings = relationship("DataSourceMapping", back_populates="source")


class DataSourceMapping(Base):
    """Data source mapping model"""
    __tablename__ = "data_source_mappings"
    
    mapping_id = Column(Integer, primary_key=True, index=True)
    source_id = Column(Integer, ForeignKey("data_sources.source_id"))
    entity_type = Column(String(50), nullable=False)
    entity_id = Column(Integer, nullable=False)
    external_id = Column(String(255), nullable=False)
    last_updated = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    source = relationship("DataSource", back_populates="mappings")


class User(Base):
    """User model"""
    __tablename__ = "users"
    
    user_id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    first_name = Column(String(100))
    last_name = Column(String(100))
    role = Column(String(50), nullable=False)
    is_active = Column(Boolean, default=True)
    last_login = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    notification_settings = relationship("NotificationSetting", back_populates="user")
    notifications = relationship("Notification", back_populates="user")
    search_history = relationship("SearchHistory", back_populates="user")
    saved_searches = relationship("SavedSearch", back_populates="user")


class NotificationSetting(Base):
    """Notification settings model"""
    __tablename__ = "notification_settings"
    
    setting_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.user_id"))
    notification_type = Column(String(50), nullable=False)
    is_enabled = Column(Boolean, default=True)
    delivery_method = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="notification_settings")


class Notification(Base):
    """Notification model"""
    __tablename__ = "notifications"
    
    notification_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.user_id"))
    notification_type = Column(String(50), nullable=False)
    entity_type = Column(String(50))
    entity_id = Column(Integer)
    message = Column(Text, nullable=False)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="notifications")


class SearchHistory(Base):
    """Search history model"""
    __tablename__ = "search_history"
    
    search_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.user_id"))
    search_query = Column(JSONB, nullable=False)
    search_date = Column(DateTime, default=datetime.utcnow)
    result_count = Column(Integer)
    
    # Relationships
    user = relationship("User", back_populates="search_history")


class SavedSearch(Base):
    """Saved search model"""
    __tablename__ = "saved_searches"
    
    saved_search_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.user_id"))
    search_name = Column(String(100), nullable=False)
    search_query = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="saved_searches")


class AuditLog(Base):
    """Audit log model"""
    __tablename__ = "audit_log"
    
    log_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.user_id"))
    action_type = Column(String(50), nullable=False)
    entity_type = Column(String(50))
    entity_id = Column(Integer)
    old_value = Column(JSONB)
    new_value = Column(JSONB)
    ip_address = Column(String(50))
    user_agent = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


#######################
# Database Views
#######################

# These will be created using raw SQL after the tables are created
VENDOR_SUMMARY_VIEW = """
CREATE VIEW vendor_summary AS
SELECT 
    v.vendor_id,
    v.company_name,
    v.country,
    v.website,
    v.data_quality_score,
    COUNT(DISTINCT vp.product_id) AS product_count,
    COUNT(DISTINCT vc.certification_id) AS certification_count,
    COUNT(DISTINCT ra.approval_id) AS regulatory_approval_count,
    ARRAY_AGG(DISTINCT c.certification_name) AS certifications,
    ARRAY_AGG(DISTINCT ra.approval_type) AS approval_types
FROM 
    vendors v
LEFT JOIN 
    vendor_products vp ON v.vendor_id = vp.vendor_id
LEFT JOIN 
    vendor_certifications vc ON v.vendor_id = vc.vendor_id
LEFT JOIN 
    certifications c ON vc.certification_id = c.certification_id
LEFT JOIN 
    regulatory_approvals ra ON v.vendor_id = ra.vendor_id
GROUP BY 
    v.vendor_id, v.company_name, v.country, v.website, v.data_quality_score;
"""

PRODUCT_VENDORS_VIEW = """
CREATE VIEW product_vendors AS
SELECT 
    p.product_id,
    p.cas_number,
    p.chemical_name,
    p.common_name,
    v.vendor_id,
    v.company_name,
    v.country,
    vp.min_order_quantity,
    vp.capacity,
    vp.product_grade,
    vc.status AS certification_status,
    c.certification_name,
    ra.approval_type,
    ra.regulatory_body,
    ra.status AS approval_status
FROM 
    products p
JOIN 
    vendor_products vp ON p.product_id = vp.product_id
JOIN 
    vendors v ON vp.vendor_id = v.vendor_id
LEFT JOIN 
    vendor_certifications vc ON v.vendor_id = vc.vendor_id
LEFT JOIN 
    certifications c ON vc.certification_id = c.certification_id
LEFT JOIN 
    regulatory_approvals ra ON v.vendor_id = ra.vendor_id AND p.product_id = ra.product_id;
"""

REGULATORY_STATUS_VIEW = """
CREATE VIEW regulatory_status AS
SELECT 
    v.vendor_id,
    v.company_name,
    p.product_id,
    p.cas_number,
    p.chemical_name,
    COUNT(CASE WHEN ra.approval_type = 'GMP' THEN 1 END) > 0 AS has_gmp,
    COUNT(CASE WHEN ra.approval_type = 'DMF' THEN 1 END) > 0 AS has_dmf,
    COUNT(CASE WHEN ra.approval_type = 'CEP' THEN 1 END) > 0 AS has_cep,
    ARRAY_AGG(DISTINCT ra.regulatory_body) AS regulatory_bodies,
    MAX(ra.updated_at) AS last_updated
FROM 
    vendors v
JOIN 
    vendor_products vp ON v.vendor_id = vp.vendor_id
JOIN 
    products p ON vp.product_id = p.product_id
LEFT JOIN 
    regulatory_approvals ra ON v.vendor_id = ra.vendor_id AND p.product_id = ra.product_id
GROUP BY 
    v.vendor_id, v.company_name, p.product_id, p.cas_number, p.chemical_name;
"""

#######################
# Database Indexes
#######################

# These will be created using raw SQL after the tables are created
INDEXES = [
    "CREATE INDEX idx_vendors_company_name ON vendors(company_name);",
    "CREATE INDEX idx_vendors_country ON vendors(country);",
    "CREATE INDEX idx_products_cas_number ON products(cas_number);",
    "CREATE INDEX idx_products_chemical_name ON products(chemical_name);",
    "CREATE INDEX idx_products_common_name ON products(common_name);",
    "CREATE INDEX idx_product_synonyms_name ON product_synonyms(synonym_name);",
    "CREATE INDEX idx_vendor_products_vendor_id ON vendor_products(vendor_id);",
    "CREATE INDEX idx_vendor_products_product_id ON vendor_products(product_id);",
    "CREATE INDEX idx_regulatory_approvals_vendor_id ON regulatory_approvals(vendor_id);",
    "CREATE INDEX idx_regulatory_approvals_product_id ON regulatory_approvals(product_id);",
    "CREATE INDEX idx_regulatory_approvals_type_body ON regulatory_approvals(approval_type, regulatory_body);",
    "CREATE INDEX idx_vendor_certifications_vendor_id ON vendor_certifications(vendor_id);",
    "CREATE INDEX idx_vendor_certifications_certification_id ON vendor_certifications(certification_id);",
    "CREATE INDEX idx_vendor_certifications_status ON vendor_certifications(status);",
    "CREATE INDEX idx_products_fts ON products USING gin(to_tsvector('english', chemical_name || ' ' || coalesce(common_name, '')));",
    "CREATE INDEX idx_vendors_fts ON vendors USING gin(to_tsvector('english', company_name || ' ' || coalesce(city, '') || ' ' || coalesce(country, '')));"
]

#######################
# Pydantic Models
#######################

class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


class UserBase(BaseModel):
    username: str
    email: EmailStr
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    role: str


class UserCreate(UserBase):
    password: str


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    password: Optional[str] = None


class UserInDB(UserBase):
    user_id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime
    last_login: Optional[datetime] = None

    class Config:
        orm_mode = True


class UserOut(UserBase):
    user_id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime
    last_login: Optional[datetime] = None

    class Config:
        orm_mode = True


class VendorContactBase(BaseModel):
    contact_name: Optional[str] = None
    position: Optional[str] = None
    department: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[EmailStr] = None
    is_primary: bool = False


class VendorContactCreate(VendorContactBase):
    vendor_id: int


class VendorContactUpdate(VendorContactBase):
    pass


class VendorContactInDB(VendorContactBase):
    contact_id: int
    vendor_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class VendorContactOut(VendorContactBase):
    contact_id: int
    vendor_id: int

    class Config:
        orm_mode = True


class VendorBase(BaseModel):
    company_name: str
    address: Optional[str] = None
    city: Optional[str] = None
    state_province: Optional[str] = None
    country: Optional[str] = None
    postal_code: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[EmailStr] = None
    website: Optional[str] = None
    year_established: Optional[int] = None
    company_size: Optional[str] = None


class VendorCreate(VendorBase):
    data_quality_score: Optional[float] = None
    last_verified_date: Optional[datetime] = None


class VendorUpdate(VendorBase):
    company_name: Optional[str] = None
    data_quality_score: Optional[float] = None
    last_verified_date: Optional[datetime] = None


class VendorInDB(VendorBase):
    vendor_id: int
    data_quality_score: Optional[float] = None
    last_verified_date: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class VendorOut(VendorBase):
    vendor_id: int
    data_quality_score: Optional[float] = None
    last_verified_date: Optional[datetime] = None
    contacts: List[VendorContactOut] = []

    class Config:
        orm_mode = True


class VendorDetailOut(VendorOut):
    certifications: List[dict] = []
    regulatory_approvals: List[dict] = []
    product_count: int = 0
    top_products: List[dict] = []

    class Config:
        orm_mode = True


class ProductBase(BaseModel):
    cas_number: Optional[str] = None
    chemical_name: str
    common_name: Optional[str] = None
    molecular_formula: Optional[str] = None
    molecular_weight: Optional[float] = None
    product_category: Optional[str] = None
    therapeutic_category: Optional[str] = None
    structure_data: Optional[dict] = None


class ProductCreate(ProductBase):
    pass


class ProductUpdate(ProductBase):
    chemical_name: Optional[str] = None


class ProductInDB(ProductBase):
    product_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class ProductOut(ProductBase):
    product_id: int
    synonyms: List[str] = []

    class Config:
        orm_mode = True


class ProductDetailOut(ProductOut):
    vendors: List[dict] = []
    regulatory_approvals: List[dict] = []

    class Config:
        orm_mode = True


class ProductSynonymBase(BaseModel):
    synonym_name: str
    synonym_type: Optional[str] = None


class ProductSynonymCreate(ProductSynonymBase):
    product_id: int


class ProductSynonymInDB(ProductSynonymBase):
    synonym_id: int
    product_id: int
    created_at: datetime

    class Config:
        orm_mode = True


class VendorProductBase(BaseModel):
    min_order_quantity: Optional[str] = None
    capacity: Optional[str] = None
    lead_time: Optional[str] = None
    product_grade: Optional[str] = None
    pricing_info: Optional[dict] = None


class VendorProductCreate(VendorProductBase):
    vendor_id: int
    product_id: int


class VendorProductUpdate(VendorProductBase):
    pass


class VendorProductInDB(VendorProductBase):
    vendor_product_id: int
    vendor_id: int
    product_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class VendorProductOut(VendorProductBase):
    vendor_product_id: int
    vendor: VendorOut
    product: ProductOut

    class Config:
        orm_mode = True


class CertificationBase(BaseModel):
    certification_name: str
    issuing_body: Optional[str] = None
    description: Optional[str] = None


class CertificationCreate(CertificationBase):
    pass


class CertificationUpdate(CertificationBase):
    certification_name: Optional[str] = None


class CertificationInDB(CertificationBase):
    certification_id: int
    created_at: datetime

    class Config:
        orm_mode = True


class CertificationOut(CertificationBase):
    certification_id: int

    class Config:
        orm_mode = True


class VendorCertificationBase(BaseModel):
    certificate_number: Optional[str] = None
    issue_date: Optional[datetime] = None
    expiry_date: Optional[datetime] = None
    status: Optional[str] = None
    document_url: Optional[str] = None


class VendorCertificationCreate(VendorCertificationBase):
    vendor_id: int
    certification_id: int


class VendorCertificationUpdate(VendorCertificationBase):
    pass


class VendorCertificationInDB(VendorCertificationBase):
    vendor_certification_id: int
    vendor_id: int
    certification_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class VendorCertificationOut(VendorCertificationBase):
    vendor_certification_id: int
    vendor_id: int
    certification: CertificationOut

    class Config:
        orm_mode = True


class RegulatoryApprovalBase(BaseModel):
    approval_type: str
    regulatory_body: str
    approval_number: Optional[str] = None
    issue_date: Optional[datetime] = None
    expiry_date: Optional[datetime] = None
    status: Optional[str] = None
    document_url: Optional[str] = None
    additional_info: Optional[dict] = None


class RegulatoryApprovalCreate(RegulatoryApprovalBase):
    vendor_id: int
    product_id: Optional[int] = None


class RegulatoryApprovalUpdate(RegulatoryApprovalBase):
    approval_type: Optional[str] = None
    regulatory_body: Optional[str] = None


class RegulatoryApprovalInDB(RegulatoryApprovalBase):
    approval_id: int
    vendor_id: int
    product_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class RegulatoryApprovalOut(RegulatoryApprovalBase):
    approval_id: int
    vendor_id: int
    product_id: Optional[int] = None

    class Config:
        orm_mode = True


class SearchQuery(BaseModel):
    query: Optional[str] = None
    cas: Optional[str] = None
    filters: Optional[dict] = None
    page: int = 1
    page_size: int = 20
    sort_by: Optional[str] = None
    sort_order: Optional[str] = "asc"


class SavedSearchBase(BaseModel):
    search_name: str
    search_query: dict


class SavedSearchCreate(SavedSearchBase):
    pass


class SavedSearchUpdate(SavedSearchBase):
    search_name: Optional[str] = None
    search_query: Optional[dict] = None


class SavedSearchInDB(SavedSearchBase):
    saved_search_id: int
    user_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class SavedSearchOut(SavedSearchBase):
    saved_search_id: int
    user_id: int
    created_at: datetime

    class Config:
        orm_mode = True


class NotificationSettingBase(BaseModel):
    notification_type: str
    is_enabled: bool = True
    delivery_method: str


class NotificationSettingCreate(NotificationSettingBase):
    pass


class NotificationSettingUpdate(NotificationSettingBase):
    notification_type: Optional[str] = None
    is_enabled: Optional[bool] = None
    delivery_method: Optional[str] = None


class NotificationSettingInDB(NotificationSettingBase):
    setting_id: int
    user_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class NotificationSettingOut(NotificationSettingBase):
    setting_id: int
    user_id: int

    class Config:
        orm_mode = True


class NotificationBase(BaseModel):
    notification_type: str
    entity_type: Optional[str] = None
    entity_id: Optional[int] = None
    message: str
    is_read: bool = False


class NotificationCreate(NotificationBase):
    user_id: int


class NotificationUpdate(BaseModel):
    is_read: Optional[bool] = None


class NotificationInDB(NotificationBase):
    notification_id: int
    user_id: int
    created_at: datetime

    class Config:
        orm_mode = True


class NotificationOut(NotificationBase):
    notification_id: int
    user_id: int
    created_at: datetime

    class Config:
        orm_mode = True


class ExportRequest(BaseModel):
    entity_type: str
    entity_ids: List[int]
    format: str
    template_id: Optional[int] = None


#######################
# Helper Functions
#######################

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def verify_password(plain_password, hashed_password):
    """Verify password against hash"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    """Generate password hash"""
    return pwd_context.hash(password)


def get_user(db, username: str):
    """Get user by username"""
    return db.query(User).filter(User.username == username).first()


def authenticate_user(db, username: str, password: str):
    """Authenticate user"""
    user = get_user(db, username)
    if not user:
        return False
    if not verify_password(password, user.password_hash):
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme), db = Depends(get_db)):
    """Get current user from token"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except jwt.PyJWTError:
        raise credentials_exception
    user = get_user(db, username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(current_user: User = Depends(get_current_user)):
    """Get current active user"""
    if not current_user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


def is_admin(user: User):
    """Check if user is admin"""
    return user.role == "admin"


async def get_admin_user(current_user: User = Depends(get_current_active_user)):
    """Get current admin user"""
    if not is_admin(current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    return current_user


def log_audit(db, user_id, action_type, entity_type, entity_id, old_value, new_value, ip_address, user_agent):
    """Log audit entry"""
    audit_log = AuditLog(
        user_id=user_id,
        action_type=action_type,
        entity_type=entity_type,
        entity_id=entity_id,
        old_value=old_value,
        new_value=new_value,
        ip_address=ip_address,
        user_agent=user_agent
    )
    db.add(audit_log)
    db.commit()


def create_notification(db, user_id, notification_type, entity_type, entity_id, message):
    """Create notification"""
    notification = Notification(
        user_id=user_id,
        notification_type=notification_type,
        entity_type=entity_type,
        entity_id=entity_id,
        message=message
    )
    db.add(notification)
    db.commit()
    db.refresh(notification)
    return notification


def build_search_query(db, search_params):
    """Build search query based on parameters"""
    # This is a simplified version - in production, this would be more complex
    query = None
    params = {}
    
    if search_params.query:
        query = """
        SELECT p.product_id, p.cas_number, p.chemical_name, p.common_name,
               v.vendor_id, v.company_name, v.country
        FROM products p
        JOIN vendor_products vp ON p.product_id = vp.product_id
        JOIN vendors v ON vp.vendor_id = v.vendor_id
        WHERE p.chemical_name ILIKE :query OR p.common_name ILIKE :query
        """
        params["query"] = f"%{search_params.query}%"
    
    elif search_params.cas:
        query = """
        SELECT p.product_id, p.cas_number, p.chemical_name, p.common_name,
               v.vendor_id, v.company_name, v.country
        FROM products p
        JOIN vendor_products vp ON p.product_id = vp.product_id
        JOIN vendors v ON vp.vendor_id = v.vendor_id
        WHERE p.cas_number = :cas
        """
        params["cas"] = search_params.cas
    
    # Add filters
    if search_params.filters:
        if "regulatory_body" in search_params.filters:
            if query:
                query += " AND ra.regulatory_body = :regulatory_body"
            else:
                query = """
                SELECT p.product_id, p.cas_number, p.chemical_name, p.common_name,
                       v.vendor_id, v.company_name, v.country
                FROM products p
                JOIN vendor_products vp ON p.product_id = vp.product_id
                JOIN vendors v ON vp.vendor_id = v.vendor_id
                JOIN regulatory_approvals ra ON v.vendor_id = ra.vendor_id AND p.product_id = ra.product_id
                WHERE ra.regulatory_body = :regulatory_body
                """
            params["regulatory_body"] = search_params.filters["regulatory_body"]
        
        if "has_gmp" in search_params.filters and search_params.filters["has_gmp"]:
            if "JOIN regulatory_approvals" not in query:
                query = query.replace(
                    "JOIN vendors v ON vp.vendor_id = v.vendor_id",
                    "JOIN vendors v ON vp.vendor_id = v.vendor_id JOIN regulatory_approvals ra ON v.vendor_id = ra.vendor_id AND p.product_id = ra.product_id"
                )
            query += " AND ra.approval_type = 'GMP'"
    
    # Add pagination
    offset = (search_params.page - 1) * search_params.page_size
    query += f" LIMIT {search_params.page_size} OFFSET {offset}"
    
    # Execute query
    result = db.execute(query, params).fetchall()
    return result


#######################
# API Endpoints
#######################

@app.post("/api/auth/login", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db = Depends(get_db)):
    """Login endpoint"""
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    
    # Update last login
    user.last_login = datetime.utcnow()
    db.commit()
    
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/api/auth/me", response_model=UserOut)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    """Get current user info"""
    return current_user


@app.get("/api/vendors", response_model=List[VendorOut])
async def list_vendors(
    name: Optional[str] = None,
    country: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """List vendors with filtering"""
    skip = (page - 1) * page_size
    query = db.query(Vendor)
    
    if name:
        query = query.filter(Vendor.company_name.ilike(f"%{name}%"))
    if country:
        query = query.filter(Vendor.country.ilike(f"%{country}%"))
    
    total = query.count()
    vendors = query.offset(skip).limit(page_size).all()
    
    return vendors


@app.get("/api/vendors/{vendor_id}", response_model=VendorDetailOut)
async def get_vendor(
    vendor_id: int = Path(..., gt=0),
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get vendor details"""
    vendor = db.query(Vendor).filter(Vendor.vendor_id == vendor_id).first()
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    # Get certifications
    certifications = db.query(VendorCertification, Certification).join(
        Certification, VendorCertification.certification_id == Certification.certification_id
    ).filter(VendorCertification.vendor_id == vendor_id).all()
    
    cert_list = []
    for vc, c in certifications:
        cert_list.append({
            "certification_id": c.certification_id,
            "certification_name": c.certification_name,
            "certificate_number": vc.certificate_number,
            "issue_date": vc.issue_date,
            "expiry_date": vc.expiry_date,
            "status": vc.status
        })
    
    # Get regulatory approvals
    approvals = db.query(RegulatoryApproval).filter(
        RegulatoryApproval.vendor_id == vendor_id
    ).all()
    
    approval_list = []
    for ra in approvals:
        approval_list.append({
            "approval_id": ra.approval_id,
            "approval_type": ra.approval_type,
            "regulatory_body": ra.regulatory_body,
            "approval_number": ra.approval_number,
            "issue_date": ra.issue_date,
            "expiry_date": ra.expiry_date,
            "status": ra.status
        })
    
    # Get products
    products = db.query(Product, VendorProduct).join(
        VendorProduct, Product.product_id == VendorProduct.product_id
    ).filter(VendorProduct.vendor_id == vendor_id).limit(5).all()
    
    product_list = []
    for p, vp in products:
        product_list.append({
            "product_id": p.product_id,
            "cas_number": p.cas_number,
            "chemical_name": p.chemical_name,
            "common_name": p.common_name
        })
    
    # Count total products
    product_count = db.query(VendorProduct).filter(
        VendorProduct.vendor_id == vendor_id
    ).count()
    
    # Create response
    response = VendorDetailOut.from_orm(vendor)
    response.certifications = cert_list
    response.regulatory_approvals = approval_list
    response.product_count = product_count
    response.top_products = product_list
    
    return response


@app.post("/api/vendors", response_model=VendorOut, status_code=status.HTTP_201_CREATED)
async def create_vendor(
    vendor: VendorCreate,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """Create new vendor (admin only)"""
    db_vendor = Vendor(**vendor.dict())
    db.add(db_vendor)
    db.commit()
    db.refresh(db_vendor)
    
    # Log audit
    log_audit(
        db=db,
        user_id=current_user.user_id,
        action_type="create",
        entity_type="vendor",
        entity_id=db_vendor.vendor_id,
        old_value=None,
        new_value=vendor.dict(),
        ip_address="127.0.0.1",  # In production, get from request
        user_agent="API"  # In production, get from request
    )
    
    return db_vendor


@app.put("/api/vendors/{vendor_id}", response_model=VendorOut)
async def update_vendor(
    vendor_id: int,
    vendor: VendorUpdate,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """Update vendor (admin only)"""
    db_vendor = db.query(Vendor).filter(Vendor.vendor_id == vendor_id).first()
    if not db_vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    # Store old values for audit
    old_values = {
        "company_name": db_vendor.company_name,
        "address": db_vendor.address,
        "city": db_vendor.city,
        "state_province": db_vendor.state_province,
        "country": db_vendor.country,
        "postal_code": db_vendor.postal_code,
        "phone": db_vendor.phone,
        "email": db_vendor.email,
        "website": db_vendor.website,
        "year_established": db_vendor.year_established,
        "company_size": db_vendor.company_size,
        "data_quality_score": db_vendor.data_quality_score,
        "last_verified_date": db_vendor.last_verified_date
    }
    
    # Update vendor
    update_data = vendor.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_vendor, key, value)
    
    db_vendor.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(db_vendor)
    
    # Log audit
    log_audit(
        db=db,
        user_id=current_user.user_id,
        action_type="update",
        entity_type="vendor",
        entity_id=vendor_id,
        old_value=old_values,
        new_value=update_data,
        ip_address="127.0.0.1",  # In production, get from request
        user_agent="API"  # In production, get from request
    )
    
    return db_vendor


@app.delete("/api/vendors/{vendor_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_vendor(
    vendor_id: int,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """Delete vendor (admin only)"""
    db_vendor = db.query(Vendor).filter(Vendor.vendor_id == vendor_id).first()
    if not db_vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    # Store for audit
    old_values = {
        "company_name": db_vendor.company_name,
        "address": db_vendor.address,
        "city": db_vendor.city,
        "state_province": db_vendor.state_province,
        "country": db_vendor.country,
        "postal_code": db_vendor.postal_code,
        "phone": db_vendor.phone,
        "email": db_vendor.email,
        "website": db_vendor.website,
        "year_established": db_vendor.year_established,
        "company_size": db_vendor.company_size,
        "data_quality_score": db_vendor.data_quality_score,
        "last_verified_date": db_vendor.last_verified_date
    }
    
    # Delete vendor
    db.delete(db_vendor)
    db.commit()
    
    # Log audit
    log_audit(
        db=db,
        user_id=current_user.user_id,
        action_type="delete",
        entity_type="vendor",
        entity_id=vendor_id,
        old_value=old_values,
        new_value=None,
        ip_address="127.0.0.1",  # In production, get from request
        user_agent="API"  # In production, get from request
    )
    
    return None


@app.get("/api/vendors/{vendor_id}/products", response_model=List[dict])
async def get_vendor_products(
    vendor_id: int,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get vendor's products"""
    vendor = db.query(Vendor).filter(Vendor.vendor_id == vendor_id).first()
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    products = db.query(Product, VendorProduct).join(
        VendorProduct, Product.product_id == VendorProduct.product_id
    ).filter(VendorProduct.vendor_id == vendor_id).all()
    
    result = []
    for p, vp in products:
        result.append({
            "product_id": p.product_id,
            "cas_number": p.cas_number,
            "chemical_name": p.chemical_name,
            "common_name": p.common_name,
            "min_order_quantity": vp.min_order_quantity,
            "capacity": vp.capacity,
            "product_grade": vp.product_grade
        })
    
    return result


@app.get("/api/vendors/{vendor_id}/certifications", response_model=List[dict])
async def get_vendor_certifications(
    vendor_id: int,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get vendor's certifications"""
    vendor = db.query(Vendor).filter(Vendor.vendor_id == vendor_id).first()
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    certifications = db.query(VendorCertification, Certification).join(
        Certification, VendorCertification.certification_id == Certification.certification_id
    ).filter(VendorCertification.vendor_id == vendor_id).all()
    
    result = []
    for vc, c in certifications:
        result.append({
            "vendor_certification_id": vc.vendor_certification_id,
            "certification_id": c.certification_id,
            "certification_name": c.certification_name,
            "issuing_body": c.issuing_body,
            "certificate_number": vc.certificate_number,
            "issue_date": vc.issue_date,
            "expiry_date": vc.expiry_date,
            "status": vc.status,
            "document_url": vc.document_url
        })
    
    return result


@app.get("/api/vendors/{vendor_id}/approvals", response_model=List[dict])
async def get_vendor_approvals(
    vendor_id: int,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get vendor's regulatory approvals"""
    vendor = db.query(Vendor).filter(Vendor.vendor_id == vendor_id).first()
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    
    approvals = db.query(RegulatoryApproval).filter(
        RegulatoryApproval.vendor_id == vendor_id
    ).all()
    
    result = []
    for ra in approvals:
        approval_dict = {
            "approval_id": ra.approval_id,
            "approval_type": ra.approval_type,
            "regulatory_body": ra.regulatory_body,
            "approval_number": ra.approval_number,
            "issue_date": ra.issue_date,
            "expiry_date": ra.expiry_date,
            "status": ra.status,
            "document_url": ra.document_url
        }
        
        # Add product info if available
        if ra.product_id:
            product = db.query(Product).filter(Product.product_id == ra.product_id).first()
            if product:
                approval_dict["product"] = {
                    "product_id": product.product_id,
                    "cas_number": product.cas_number,
                    "chemical_name": product.chemical_name,
                    "common_name": product.common_name
                }
        
        result.append(approval_dict)
    
    return result


@app.get("/api/products", response_model=List[ProductOut])
async def list_products(
    cas: Optional[str] = None,
    name: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """List products with filtering"""
    skip = (page - 1) * page_size
    query = db.query(Product)
    
    if cas:
        query = query.filter(Product.cas_number == cas)
    if name:
        query = query.filter(
            (Product.chemical_name.ilike(f"%{name}%")) | 
            (Product.common_name.ilike(f"%{name}%"))
        )
    
    total = query.count()
    products = query.offset(skip).limit(page_size).all()
    
    # Get synonyms for each product
    result = []
    for product in products:
        synonyms = db.query(ProductSynonym).filter(
            ProductSynonym.product_id == product.product_id
        ).all()
        
        product_dict = ProductOut.from_orm(product)
        product_dict.synonyms = [s.synonym_name for s in synonyms]
        result.append(product_dict)
    
    return result


@app.get("/api/products/{product_id}", response_model=ProductDetailOut)
async def get_product(
    product_id: int = Path(..., gt=0),
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get product details"""
    product = db.query(Product).filter(Product.product_id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Get synonyms
    synonyms = db.query(ProductSynonym).filter(
        ProductSynonym.product_id == product_id
    ).all()
    
    # Get vendors
    vendors = db.query(Vendor, VendorProduct).join(
        VendorProduct, Vendor.vendor_id == VendorProduct.vendor_id
    ).filter(VendorProduct.product_id == product_id).all()
    
    vendor_list = []
    for v, vp in vendors:
        # Get regulatory status for this vendor-product combination
        approvals = db.query(RegulatoryApproval).filter(
            RegulatoryApproval.vendor_id == v.vendor_id,
            RegulatoryApproval.product_id == product_id
        ).all()
        
        has_gmp = any(a.approval_type == 'GMP' for a in approvals)
        has_dmf = any(a.approval_type == 'DMF' for a in approvals)
        has_cep = any(a.approval_type == 'CEP' for a in approvals)
        regulatory_bodies = list(set(a.regulatory_body for a in approvals))
        
        vendor_list.append({
            "vendor_id": v.vendor_id,
            "company_name": v.company_name,
            "country": v.country,
            "min_order_quantity": vp.min_order_quantity,
            "capacity": vp.capacity,
            "product_grade": vp.product_grade,
            "has_gmp": has_gmp,
            "has_dmf": has_dmf,
            "has_cep": has_cep,
            "regulatory_bodies": regulatory_bodies
        })
    
    # Get regulatory approvals
    approvals = db.query(RegulatoryApproval).filter(
        RegulatoryApproval.product_id == product_id
    ).all()
    
    approval_list = []
    for ra in approvals:
        vendor = db.query(Vendor).filter(Vendor.vendor_id == ra.vendor_id).first()
        approval_list.append({
            "approval_id": ra.approval_id,
            "vendor_id": ra.vendor_id,
            "vendor_name": vendor.company_name if vendor else None,
            "approval_type": ra.approval_type,
            "regulatory_body": ra.regulatory_body,
            "approval_number": ra.approval_number,
            "issue_date": ra.issue_date,
            "expiry_date": ra.expiry_date,
            "status": ra.status
        })
    
    # Create response
    response = ProductDetailOut.from_orm(product)
    response.synonyms = [s.synonym_name for s in synonyms]
    response.vendors = vendor_list
    response.regulatory_approvals = approval_list
    
    return response


@app.post("/api/products", response_model=ProductOut, status_code=status.HTTP_201_CREATED)
async def create_product(
    product: ProductCreate,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """Create new product (admin only)"""
    # Check if CAS number already exists
    if product.cas_number:
        existing = db.query(Product).filter(Product.cas_number == product.cas_number).first()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Product with CAS number {product.cas_number} already exists"
            )
    
    db_product = Product(**product.dict())
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    
    # Log audit
    log_audit(
        db=db,
        user_id=current_user.user_id,
        action_type="create",
        entity_type="product",
        entity_id=db_product.product_id,
        old_value=None,
        new_value=product.dict(),
        ip_address="127.0.0.1",  # In production, get from request
        user_agent="API"  # In production, get from request
    )
    
    return db_product


@app.put("/api/products/{product_id}", response_model=ProductOut)
async def update_product(
    product_id: int,
    product: ProductUpdate,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """Update product (admin only)"""
    db_product = db.query(Product).filter(Product.product_id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Check if CAS number already exists (if being updated)
    if product.cas_number and product.cas_number != db_product.cas_number:
        existing = db.query(Product).filter(Product.cas_number == product.cas_number).first()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Product with CAS number {product.cas_number} already exists"
            )
    
    # Store old values for audit
    old_values = {
        "cas_number": db_product.cas_number,
        "chemical_name": db_product.chemical_name,
        "common_name": db_product.common_name,
        "molecular_formula": db_product.molecular_formula,
        "molecular_weight": db_product.molecular_weight,
        "product_category": db_product.product_category,
        "therapeutic_category": db_product.therapeutic_category,
        "structure_data": db_product.structure_data
    }
    
    # Update product
    update_data = product.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_product, key, value)
    
    db_product.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(db_product)
    
    # Log audit
    log_audit(
        db=db,
        user_id=current_user.user_id,
        action_type="update",
        entity_type="product",
        entity_id=product_id,
        old_value=old_values,
        new_value=update_data,
        ip_address="127.0.0.1",  # In production, get from request
        user_agent="API"  # In production, get from request
    )
    
    # Get synonyms for response
    synonyms = db.query(ProductSynonym).filter(
        ProductSynonym.product_id == product_id
    ).all()
    
    response = ProductOut.from_orm(db_product)
    response.synonyms = [s.synonym_name for s in synonyms]
    
    return response


@app.delete("/api/products/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_product(
    product_id: int,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """Delete product (admin only)"""
    db_product = db.query(Product).filter(Product.product_id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Store for audit
    old_values = {
        "cas_number": db_product.cas_number,
        "chemical_name": db_product.chemical_name,
        "common_name": db_product.common_name,
        "molecular_formula": db_product.molecular_formula,
        "molecular_weight": db_product.molecular_weight,
        "product_category": db_product.product_category,
        "therapeutic_category": db_product.therapeutic_category
    }
    
    # Delete product
    db.delete(db_product)
    db.commit()
    
    # Log audit
    log_audit(
        db=db,
        user_id=current_user.user_id,
        action_type="delete",
        entity_type="product",
        entity_id=product_id,
        old_value=old_values,
        new_value=None,
        ip_address="127.0.0.1",  # In production, get from request
        user_agent="API"  # In production, get from request
    )
    
    return None


@app.get("/api/products/{product_id}/vendors", response_model=List[dict])
async def get_product_vendors(
    product_id: int,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get vendors for a product"""
    product = db.query(Product).filter(Product.product_id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    vendors = db.query(Vendor, VendorProduct).join(
        VendorProduct, Vendor.vendor_id == VendorProduct.vendor_id
    ).filter(VendorProduct.product_id == product_id).all()
    
    result = []
    for v, vp in vendors:
        # Get regulatory status for this vendor-product combination
        approvals = db.query(RegulatoryApproval).filter(
            RegulatoryApproval.vendor_id == v.vendor_id,
            RegulatoryApproval.product_id == product_id
        ).all()
        
        has_gmp = any(a.approval_type == 'GMP' for a in approvals)
        has_dmf = any(a.approval_type == 'DMF' for a in approvals)
        has_cep = any(a.approval_type == 'CEP' for a in approvals)
        regulatory_bodies = list(set(a.regulatory_body for a in approvals))
        
        result.append({
            "vendor_id": v.vendor_id,
            "company_name": v.company_name,
            "country": v.country,
            "min_order_quantity": vp.min_order_quantity,
            "capacity": vp.capacity,
            "product_grade": vp.product_grade,
            "has_gmp": has_gmp,
            "has_dmf": has_dmf,
            "has_cep": has_cep,
            "regulatory_bodies": regulatory_bodies
        })
    
    return result


@app.get("/api/products/{product_id}/approvals", response_model=List[dict])
async def get_product_approvals(
    product_id: int,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get regulatory approvals for a product"""
    product = db.query(Product).filter(Product.product_id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    approvals = db.query(RegulatoryApproval).filter(
        RegulatoryApproval.product_id == product_id
    ).all()
    
    result = []
    for ra in approvals:
        vendor = db.query(Vendor).filter(Vendor.vendor_id == ra.vendor_id).first()
        result.append({
            "approval_id": ra.approval_id,
            "vendor_id": ra.vendor_id,
            "vendor_name": vendor.company_name if vendor else None,
            "approval_type": ra.approval_type,
            "regulatory_body": ra.regulatory_body,
            "approval_number": ra.approval_number,
            "issue_date": ra.issue_date,
            "expiry_date": ra.expiry_date,
            "status": ra.status,
            "document_url": ra.document_url
        })
    
    return result


@app.post("/api/search/vendors", response_model=dict)
async def search_vendors(
    search_params: SearchQuery,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Search vendors with advanced filtering"""
    skip = (search_params.page - 1) * search_params.page_size
    query = db.query(Vendor)
    
    # Apply filters
    if search_params.query:
        query = query.filter(Vendor.company_name.ilike(f"%{search_params.query}%"))
    
    if search_params.filters:
        if "country" in search_params.filters:
            query = query.filter(Vendor.country == search_params.filters["country"])
        
        if "certification" in search_params.filters:
            cert_name = search_params.filters["certification"]
            query = query.join(VendorCertification, Vendor.vendor_id == VendorCertification.vendor_id)
            query = query.join(Certification, VendorCertification.certification_id == Certification.certification_id)
            query = query.filter(Certification.certification_name == cert_name)
    
    # Count total
    total = query.count()
    
    # Apply pagination
    vendors = query.offset(skip).limit(search_params.page_size).all()
    
    # Save search history
    search_history = SearchHistory(
        user_id=current_user.user_id,
        search_query=search_params.dict(),
        result_count=total
    )
    db.add(search_history)
    db.commit()
    
    return {
        "count": total,
        "page": search_params.page,
        "total_pages": (total + search_params.page_size - 1) // search_params.page_size,
        "results": vendors
    }


@app.post("/api/search/products", response_model=dict)
async def search_products(
    search_params: SearchQuery,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Search products with advanced filtering"""
    # This is a simplified implementation - in production, this would be more complex
    results = build_search_query(db, search_params)
    
    # Format results
    formatted_results = []
    for row in results:
        formatted_results.append({
            "product_id": row.product_id,
            "cas_number": row.cas_number,
            "chemical_name": row.chemical_name,
            "common_name": row.common_name,
            "vendor_id": row.vendor_id,
            "company_name": row.company_name,
            "country": row.country
        })
    
    # Save search history
    search_history = SearchHistory(
        user_id=current_user.user_id,
        search_query=search_params.dict(),
        result_count=len(formatted_results)
    )
    db.add(search_history)
    db.commit()
    
    return {
        "count": len(formatted_results),
        "page": search_params.page,
        "total_pages": 1,  # Simplified - in production, calculate properly
        "results": formatted_results
    }


@app.get("/api/search/history", response_model=List[dict])
async def get_search_history(
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get user's search history"""
    history = db.query(SearchHistory).filter(
        SearchHistory.user_id == current_user.user_id
    ).order_by(SearchHistory.search_date.desc()).limit(20).all()
    
    result = []
    for item in history:
        result.append({
            "search_id": item.search_id,
            "search_query": item.search_query,
            "search_date": item.search_date,
            "result_count": item.result_count
        })
    
    return result


@app.post("/api/search/save", response_model=SavedSearchOut)
async def save_search(
    search: SavedSearchCreate,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Save a search"""
    saved_search = SavedSearch(
        user_id=current_user.user_id,
        search_name=search.search_name,
        search_query=search.search_query
    )
    db.add(saved_search)
    db.commit()
    db.refresh(saved_search)
    
    return saved_search


@app.get("/api/search/saved", response_model=List[SavedSearchOut])
async def get_saved_searches(
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get user's saved searches"""
    searches = db.query(SavedSearch).filter(
        SavedSearch.user_id == current_user.user_id
    ).order_by(SavedSearch.created_at.desc()).all()
    
    return searches


@app.delete("/api/search/saved/{saved_search_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_saved_search(
    saved_search_id: int,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Delete a saved search"""
    saved_search = db.query(SavedSearch).filter(
        SavedSearch.saved_search_id == saved_search_id,
        SavedSearch.user_id == current_user.user_id
    ).first()
    
    if not saved_search:
        raise HTTPException(status_code=404, detail="Saved search not found")
    
    db.delete(saved_search)
    db.commit()
    
    return None


@app.get("/api/notifications", response_model=List[NotificationOut])
async def get_notifications(
    is_read: Optional[bool] = None,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get user's notifications"""
    query = db.query(Notification).filter(Notification.user_id == current_user.user_id)
    
    if is_read is not None:
        query = query.filter(Notification.is_read == is_read)
    
    notifications = query.order_by(Notification.created_at.desc()).all()
    
    return notifications


@app.put("/api/notifications/{notification_id}/read")
async def mark_notification_read(
    notification_id: int,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Mark notification as read"""
    notification = db.query(Notification).filter(
        Notification.notification_id == notification_id,
        Notification.user_id == current_user.user_id
    ).first()
    
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")
    
    notification.is_read = True
    db.commit()
    
    return {"status": "success"}


@app.get("/api/notifications/settings", response_model=List[NotificationSettingOut])
async def get_notification_settings(
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Get notification settings"""
    settings = db.query(NotificationSetting).filter(
        NotificationSetting.user_id == current_user.user_id
    ).all()
    
    return settings


@app.put("/api/notifications/settings", response_model=List[NotificationSettingOut])
async def update_notification_settings(
    settings: List[NotificationSettingUpdate],
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Update notification settings"""
    # Get existing settings
    existing_settings = db.query(NotificationSetting).filter(
        NotificationSetting.user_id == current_user.user_id
    ).all()
    
    # Create mapping for easy access
    settings_map = {s.notification_type: s for s in existing_settings}
    
    # Update or create settings
    result = []
    for setting in settings:
        if setting.notification_type in settings_map:
            # Update existing
            db_setting = settings_map[setting.notification_type]
            update_data = setting.dict(exclude_unset=True)
            for key, value in update_data.items():
                setattr(db_setting, key, value)
            db_setting.updated_at = datetime.utcnow()
            result.append(db_setting)
        else:
            # Create new
            db_setting = NotificationSetting(
                user_id=current_user.user_id,
                **setting.dict()
            )
            db.add(db_setting)
            result.append(db_setting)
    
    db.commit()
    
    # Refresh all settings
    for i, setting in enumerate(result):
        db.refresh(setting)
    
    return result


@app.post("/api/export/excel")
async def export_to_excel(
    export_request: ExportRequest,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Export data to Excel"""
    # This is a placeholder - in production, implement actual Excel export
    return {
        "status": "success",
        "message": "Export initiated",
        "download_url": f"/api/export/download/{current_user.user_id}_{datetime.utcnow().timestamp()}.xlsx"
    }


@app.post("/api/export/pdf")
async def export_to_pdf(
    export_request: ExportRequest,
    current_user: User = Depends(get_current_active_user),
    db = Depends(get_db)
):
    """Export data to PDF"""
    # This is a placeholder - in production, implement actual PDF export
    return {
        "status": "success",
        "message": "Export initiated",
        "download_url": f"/api/export/download/{current_user.user_id}_{datetime.utcnow().timestamp()}.pdf"
    }


@app.get("/api/admin/users", response_model=List[UserOut])
async def list_users(
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """List users (admin only)"""
    users = db.query(User).all()
    return users


@app.post("/api/admin/users", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def create_user(
    user: UserCreate,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """Create user (admin only)"""
    # Check if username or email already exists
    existing = db.query(User).filter(
        (User.username == user.username) | (User.email == user.email)
    ).first()
    
    if existing:
        if existing.username == user.username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already registered"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
    
    # Create user
    hashed_password = get_password_hash(user.password)
    db_user = User(
        username=user.username,
        email=user.email,
        password_hash=hashed_password,
        first_name=user.first_name,
        last_name=user.last_name,
        role=user.role
    )
    
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    
    # Create default notification settings
    notification_types = ["approval_change", "data_conflict", "system_update"]
    for nt in notification_types:
        setting = NotificationSetting(
            user_id=db_user.user_id,
            notification_type=nt,
            is_enabled=True,
            delivery_method="in_app"
        )
        db.add(setting)
    
    db.commit()
    
    # Log audit
    log_audit(
        db=db,
        user_id=current_user.user_id,
        action_type="create",
        entity_type="user",
        entity_id=db_user.user_id,
        old_value=None,
        new_value={
            "username": user.username,
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "role": user.role
        },
        ip_address="127.0.0.1",  # In production, get from request
        user_agent="API"  # In production, get from request
    )
    
    return db_user


@app.put("/api/admin/users/{user_id}", response_model=UserOut)
async def update_user(
    user_id: int,
    user: UserUpdate,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """Update user (admin only)"""
    db_user = db.query(User).filter(User.user_id == user_id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Check email uniqueness if being updated
    if user.email and user.email != db_user.email:
        existing = db.query(User).filter(User.email == user.email).first()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
    
    # Store old values for audit
    old_values = {
        "email": db_user.email,
        "first_name": db_user.first_name,
        "last_name": db_user.last_name,
        "role": db_user.role,
        "is_active": db_user.is_active
    }
    
    # Update user
    update_data = user.dict(exclude_unset=True, exclude={"password"})
    for key, value in update_data.items():
        setattr(db_user, key, value)
    
    # Update password if provided
    if user.password:
        db_user.password_hash = get_password_hash(user.password)
    
    db_user.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(db_user)
    
    # Log audit
    log_audit(
        db=db,
        user_id=current_user.user_id,
        action_type="update",
        entity_type="user",
        entity_id=user_id,
        old_value=old_values,
        new_value=update_data,
        ip_address="127.0.0.1",  # In production, get from request
        user_agent="API"  # In production, get from request
    )
    
    return db_user


@app.delete("/api/admin/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: int,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """Delete user (admin only)"""
    # Prevent self-deletion
    if user_id == current_user.user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )
    
    db_user = db.query(User).filter(User.user_id == user_id).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Store for audit
    old_values = {
        "username": db_user.username,
        "email": db_user.email,
        "first_name": db_user.first_name,
        "last_name": db_user.last_name,
        "role": db_user.role,
        "is_active": db_user.is_active
    }
    
    # Delete user
    db.delete(db_user)
    db.commit()
    
    # Log audit
    log_audit(
        db=db,
        user_id=current_user.user_id,
        action_type="delete",
        entity_type="user",
        entity_id=user_id,
        old_value=old_values,
        new_value=None,
        ip_address="127.0.0.1",  # In production, get from request
        user_agent="API"  # In production, get from request
    )
    
    return None


@app.get("/api/admin/audit-log", response_model=List[dict])
async def view_audit_log(
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    user_id: Optional[int] = None,
    action_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """View audit log (admin only)"""
    skip = (page - 1) * page_size
    query = db.query(AuditLog)
    
    # Apply filters
    if entity_type:
        query = query.filter(AuditLog.entity_type == entity_type)
    if entity_id:
        query = query.filter(AuditLog.entity_id == entity_id)
    if user_id:
        query = query.filter(AuditLog.user_id == user_id)
    if action_type:
        query = query.filter(AuditLog.action_type == action_type)
    if start_date:
        start = datetime.fromisoformat(start_date)
        query = query.filter(AuditLog.created_at >= start)
    if end_date:
        end = datetime.fromisoformat(end_date)
        query = query.filter(AuditLog.created_at <= end)
    
    # Order by most recent first
    query = query.order_by(AuditLog.created_at.desc())
    
    # Count total
    total = query.count()
    
    # Apply pagination
    logs = query.offset(skip).limit(page_size).all()
    
    # Format results
    result = []
    for log in logs:
        # Get username
        user = db.query(User).filter(User.user_id == log.user_id).first()
        username = user.username if user else "Unknown"
        
        result.append({
            "log_id": log.log_id,
            "user_id": log.user_id,
            "username": username,
            "action_type": log.action_type,
            "entity_type": log.entity_type,
            "entity_id": log.entity_id,
            "old_value": log.old_value,
            "new_value": log.new_value,
            "ip_address": log.ip_address,
            "created_at": log.created_at
        })
    
    return result


@app.get("/api/admin/data-sources", response_model=List[dict])
async def list_data_sources(
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """List data sources (admin only)"""
    sources = db.query(DataSource).all()
    
    result = []
    for source in sources:
        result.append({
            "source_id": source.source_id,
            "source_name": source.source_name,
            "source_type": source.source_type,
            "source_url": source.source_url,
            "last_sync_time": source.last_sync_time,
            "sync_frequency": source.sync_frequency
        })
    
    return result


@app.post("/api/admin/data-sources/{source_id}/sync")
async def trigger_data_source_sync(
    source_id: int,
    current_user: User = Depends(get_admin_user),
    db = Depends(get_db)
):
    """Trigger data source sync (admin only)"""
    source = db.query(DataSource).filter(DataSource.source_id == source_id).first()
    if not source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # This is a placeholder - in production, implement actual sync logic
    # For now, just update the last_sync_time
    source.last_sync_time = datetime.utcnow()
    db.commit()
    
    return {
        "status": "success",
        "message": f"Sync initiated for {source.source_name}",
        "sync_id": f"sync_{source_id}_{datetime.utcnow().timestamp()}"
    }


#######################
# Database Initialization
#######################

def init_db():
    """Initialize database"""
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    # Create views and indexes
    with engine.connect() as conn:
        # Create views
        conn.execute(VENDOR_SUMMARY_VIEW)
        conn.execute(PRODUCT_VENDORS_VIEW)
        conn.execute(REGULATORY_STATUS_VIEW)
        
        # Create indexes
        for index in INDEXES:
            conn.execute(index)
    
    # Create admin user if not exists
    db = SessionLocal()
    try:
        admin = db.query(User).filter(User.username == "admin").first()
        if not admin:
            hashed_password = get_password_hash("admin")  # Change in production
            admin_user = User(
                username="admin",
                email="admin@advintpharma.com",
                password_hash=hashed_password,
                first_name="Admin",
                last_name="User",
                role="admin"
            )
            db.add(admin_user)
            db.commit()
            db.refresh(admin_user)
            
            # Create default notification settings
            notification_types = ["approval_change", "data_conflict", "system_update"]
            for nt in notification_types:
                setting = NotificationSetting(
                    user_id=admin_user.user_id,
                    notification_type=nt,
                    is_enabled=True,
                    delivery_method="in_app"
                )
                db.add(setting)
            
            db.commit()
    finally:
        db.close()


#######################
# Main Entry Point
#######################

if __name__ == "__main__":
    # Initialize database
    init_db()
    
    # Start API server
    uvicorn.run(app, host="0.0.0.0", port=8000)
