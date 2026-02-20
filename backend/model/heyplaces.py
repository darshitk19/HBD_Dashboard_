from extensions import db

class HeyPlaces(db.Model):
    __tablename__ = 'heyplaces'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255))
    address = db.Column(db.Text)
    number = db.Column(db.String(100))
    website = db.Column(db.String(512))
    category = db.Column(db.String(100))
    city = db.Column(db.String(100))
    source = db.Column(db.String(100))
    name_address_city_hash = db.Column(db.String(64))

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "address": self.address,
            "number": self.number,
            "website": self.website,
            "category": self.category,
            "city": self.city,
            "source": self.source,
            "name_address_city_hash": self.name_address_city_hash
        }