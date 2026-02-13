from extensions import db

class ATM(db.Model):
    __tablename__ = 'atm'

    id = db.Column(db.Integer, primary_key=True)
    # Changed 'name' to 'bank' to match your database image
    bank = db.Column(db.String(255)) 
    address = db.Column(db.Text)
    city = db.Column(db.String(100))
    state = db.Column(db.String(100))
    country = db.Column(db.String(100))
    category = db.Column(db.String(100))
    source = db.Column(db.String(100))
    bank_address_hash = db.Column(db.String(255))

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.bank, # Map 'bank' to 'name' for your frontend
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "category": self.category
        }