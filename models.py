from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class Lead(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    # Basic Permit Information
    permit_number = db.Column(db.String(10), unique=True)
    project_type = db.Column(db.String(80), nullable=False)  # PERMIT_TYPE
    permit_status = db.Column(db.String(80), default='Pending')  # PERMIT_STATUS
    permit_description = db.Column(db.String(200))  # PERMIT_DESCRIPTION
    
    # Dates
    submission_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)  # APPLICATION_DATE
    issue_date = db.Column(db.DateTime)  # ISSUE_DATE
    expiry_date = db.Column(db.DateTime)  # EXPIRY_DATE
    
    # Project Details
    location = db.Column(db.String(100), nullable=False)
    work_type = db.Column(db.String(80))  # WORK_TYPE (e.g., "Addition", "Interior Alteration")
    sub_work_type = db.Column(db.String(80))  # SUB_WORK_TYPE
    construction_value = db.Column(db.Float)  # Actual value instead of range
    total_units = db.Column(db.Integer)
    units_created = db.Column(db.Integer, default=0)
    
    # Property Information
    legal_description = db.Column(db.String(2000))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    
    # Stakeholders
    owner = db.Column(db.String(255))
    applicant = db.Column(db.String(250))
    contractor = db.Column(db.String(250))
    contractor_contact = db.Column(db.String(250))

    def to_dict(self):
        return {
            'id': self.id,
            'permit_number': self.permit_number,
            'project_type': self.project_type,
            'permit_status': self.permit_status,
            'permit_description': self.permit_description,
            'submission_date': self.submission_date,
            'issue_date': self.issue_date,
            'expiry_date': self.expiry_date,
            'location': self.location,
            'work_type': self.work_type,
            'sub_work_type': self.sub_work_type,
            'construction_value': self.construction_value,
            'total_units': self.total_units,
            'units_created': self.units_created,
            'legal_description': self.legal_description,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'owner': self.owner,
            'applicant': self.applicant,
            'contractor': self.contractor,
            'contractor_contact': self.contractor_contact
        } 