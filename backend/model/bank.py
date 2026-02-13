from extensions import db

class Bank(db.Model):
    __tablename__ = 'bank_data'

    id = db.Column(db.Integer, primary_key=True)
    bank = db.Column(db.String(150))
    ifsc = db.Column(db.String(50))
    micr = db.Column(db.String(9))
    branch_code = db.Column(db.String(20))
    branch = db.Column(db.String(150))
    address = db.Column(db.Text)
    city = db.Column(db.String(100))
    district = db.Column(db.String(100))
    state = db.Column(db.String(100))
    contact = db.Column(db.String(1024))
    source = db.Column(db.String(100))
    bank_branch_code_hash = db.Column(db.String(64))

    def to_dict(self):
        return {
            "id": self.id,
            "bank": self.bank,
            "ifsc": self.ifsc,
            "micr": self.micr,
            "branch": self.branch,
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "contact": self.contact
        }