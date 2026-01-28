from extensions import db
from datetime import datetime

class ScraperTask(db.Model):
    __tablename__ = 'scraper_tasks'
    
    id = db.Column(db.Integer, primary_key=True)
    platform = db.Column(db.String(50))
    search_query = db.Column(db.String(255))
    location = db.Column(db.String(255))
    status = db.Column(db.String(20), default="PENDING")
    progress = db.Column(db.Integer, default=0)
    last_index = db.Column(db.Integer, default=0)
    total_found = db.Column(db.Integer, default=0)
    should_stop = db.Column(db.Boolean, default=False)
    error_message = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<ScraperTask {self.id} - {self.status}>'