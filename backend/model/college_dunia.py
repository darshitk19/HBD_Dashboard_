from extensions import db

class CollegeDunia(db.Model):
    __tablename__ = 'college_dunia'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(150))
    address = db.Column(db.Text)
    area = db.Column(db.Text)
    avg_fees = db.Column(db.String(50))
    rating = db.Column(db.Numeric(4, 2))
    number = db.Column(db.String(100))
    website = db.Column(db.String(512))
    country = db.Column(db.String(20))
    subcategory = db.Column(db.String(15))
    category = db.Column(db.String(20))
    course_details = db.Column(db.Text)
    duration = db.Column(db.String(100))
    email = db.Column(db.String(255))
    requirement = db.Column(db.Text)
    source = db.Column(db.String(100))
    name_address_hash = db.Column(db.String(64))

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "address": self.address,
            "area": self.area,
            "avg_fees": self.avg_fees,
            "rating": float(self.rating) if self.rating else 0,
            "website": self.website,
            "course_details": self.course_details,
            "email": self.email
        }