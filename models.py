from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class Lead(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    # Basic Permit Information
    permit_number = db.Column(db.String(10), unique=True)
    permit_type = db.Column(db.String(80))
    permit_status = db.Column(db.String(80), default='Pending')
    permit_description = db.Column(db.String(200))
    
    # Project Details
    project_type = db.Column(db.String(50), nullable=False)
    work_type = db.Column(db.String(80))
    sub_work_type = db.Column(db.String(80))
    construction_value = db.Column(db.Float)
    total_units = db.Column(db.String(100))
    units_created = db.Column(db.String(100))
    units_net_change = db.Column(db.Integer)
    
    # Location Information
    location = db.Column(db.String(100), nullable=False)
    parcel_id = db.Column(db.String(50))
    legal_description = db.Column(db.String(2000))
    roll_number = db.Column(db.String(19))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    
    # Stakeholder Information
    owner = db.Column(db.String(255))
    applicant = db.Column(db.String(250))
    contractor = db.Column(db.String(250))
    contractor_contact = db.Column(db.String(250))
    
    # Dates
    submission_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    application_date = db.Column(db.DateTime)
    issue_date = db.Column(db.DateTime)
    final_date = db.Column(db.DateTime)
    expiry_date = db.Column(db.DateTime)
    
    # Additional Details
    special_conditions = db.Column(db.Text)
    permit_fee = db.Column(db.Float)
    value_range = db.Column(db.String(100), nullable=False)
    
    def to_dict(self):
        return {
            'id': self.id,
            'permit_number': self.permit_number,
            'permit_type': self.permit_type,
            'permit_status': self.permit_status,
            'project_type': self.project_type,
            'work_type': self.work_type,
            'sub_work_type': self.sub_work_type,
            'construction_value': self.construction_value,
            'location': self.location,
            'value_range': self.value_range,
            'submission_date': self.submission_date,
            'owner': self.owner,
            'applicant': self.applicant,
            'contractor': self.contractor,
            'permit_description': self.permit_description,
            'coordinates': {'lat': self.latitude, 'lng': self.longitude} if self.latitude and self.longitude else None
        } 