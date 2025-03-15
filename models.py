from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class Lead(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_type = db.Column(db.String(50), nullable=False)
    location = db.Column(db.String(100), nullable=False)
    value_range = db.Column(db.String(100), nullable=False)
    submission_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'project_type': self.project_type,
            'location': self.location,
            'value_range': self.value_range,
            'submission_date': self.submission_date
        } 