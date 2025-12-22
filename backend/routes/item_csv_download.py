# import csv
# import io
# from flask import Response, Blueprint
# from sqlalchemy import or_, and_
# from database.session import SessionLocal
# from model.item_csv_model import ItemData

# item_csv_bp = Blueprint("item_csv", __name__)

# #  Helper: streaming CSV generator
# def generate_csv(query):
#     output = io.StringIO()
#     writer = csv.writer(output)

#     # header row (all DB columns)
#     writer.writerow([col.name for col in ItemData.__table__.columns])
#     yield output.getvalue()
#     output.seek(0)
#     output.truncate(0)

#     # data rows in batches
#     for item in query.yield_per(1000):  # batch of 1000
#         writer.writerow([getattr(item, col.name) for col in ItemData.__table__.columns])
#         yield output.getvalue()
#         output.seek(0)
#         output.truncate(0)


# #  Complete CSV Download API
# @item_csv_bp.route("/items/complete/csv", methods=["GET"])
# def download_complete_csv():
#     db = SessionLocal()
#     try:
#         query = db.query(ItemData).filter(
#             ItemData.name.isnot(None), ItemData.name != "",
#             or_(
#                 and_(ItemData.phone_no_1.isnot(None), ItemData.phone_no_1 != ""),
#                 and_(ItemData.phone_no_2.isnot(None), ItemData.phone_no_2 != ""),
#                 and_(ItemData.phone_no_3.isnot(None), ItemData.phone_no_3 != ""),
#                 and_(ItemData.whatsapp_no.isnot(None), ItemData.whatsapp_no != ""),
#                 and_(ItemData.virtual_phone_no.isnot(None), ItemData.virtual_phone_no != "")
#             ),
#             ItemData.category.isnot(None), ItemData.category != "",
#             ItemData.sub_category.isnot(None), ItemData.sub_category != "",
#             or_(
#                 and_(ItemData.address.isnot(None), ItemData.address != ""),
#                 and_(ItemData.area.isnot(None), ItemData.area != "")
#             ),
#             ItemData.city.isnot(None), ItemData.city != ""
#         )

#         headers = {"Content-Disposition": "attachment; filename=complete_items.csv"}
#         return Response(generate_csv(query), mimetype="text/csv", headers=headers)
#     finally:
#         db.close()


# #  Incomplete CSV Download API
# @item_csv_bp.route("/items/incomplete/csv", methods=["GET"])
# def download_incomplete_csv():
#     db = SessionLocal()
#     try:
#         query = db.query(ItemData).filter(
#             or_(
#                 (ItemData.name.is_(None)) | (ItemData.name == ""),
#                 and_(
#                     (ItemData.phone_no_1.is_(None)) | (ItemData.phone_no_1 == ""),
#                     (ItemData.phone_no_2.is_(None)) | (ItemData.phone_no_2 == ""),
#                     (ItemData.phone_no_3.is_(None)) | (ItemData.phone_no_3 == ""),
#                     (ItemData.whatsapp_no.is_(None)) | (ItemData.whatsapp_no == ""),
#                     (ItemData.virtual_phone_no.is_(None)) | (ItemData.virtual_phone_no == "")
#                 ),
#                 (ItemData.category.is_(None)) | (ItemData.category == ""),
#                 (ItemData.sub_category.is_(None)) | (ItemData.sub_category == ""),
#                 or_(
#                     and_(ItemData.address.is_(None), ItemData.area.is_(None)),
#                     and_(ItemData.address == "", ItemData.area == "")
#                 ),
#                 (ItemData.city.is_(None)) | (ItemData.city == "")
#             )
#         )

#         headers = {"Content-Disposition": "attachment; filename=incomplete_items.csv"}
#         return Response(generate_csv(query), mimetype="text/csv", headers=headers)
#     finally:
#         db.close()


from flask import Response, stream_with_context,Blueprint
import csv
from io import StringIO
from sqlalchemy import or_, and_
from database.session import SessionLocal
from model.item_csv_model import ItemData

item_csv_bp = Blueprint("item_csv", __name__)
@item_csv_bp.route("/items/incomplete/csv", methods=["GET"])
def download_incomplete_csv():
    db = SessionLocal()

    def generate():
        try:
            query = db.query(ItemData).filter(
                or_(
                    (ItemData.name.is_(None)) | (ItemData.name == ""),
                    and_(
                        (ItemData.phone_no_1.is_(None)) | (ItemData.phone_no_1 == ""),
                        (ItemData.phone_no_2.is_(None)) | (ItemData.phone_no_2 == ""),
                        (ItemData.phone_no_3.is_(None)) | (ItemData.phone_no_3 == ""),
                        (ItemData.whatsapp_no.is_(None)) | (ItemData.whatsapp_no == ""),
                        (ItemData.virtual_phone_no.is_(None)) | (ItemData.virtual_phone_no == "")
                    ),
                    (ItemData.category.is_(None)) | (ItemData.category == ""),
                    (ItemData.sub_category.is_(None)) | (ItemData.sub_category == ""),
                    or_(
                        and_(ItemData.address.is_(None), ItemData.area.is_(None)),
                        and_(ItemData.address == "", ItemData.area == "")
                    ),
                    (ItemData.city.is_(None)) | (ItemData.city == "")
                )
            ).yield_per(100000)  # 👈 1 लाख rows प्रति chunk

            # Write headers
            buffer = StringIO()
            writer = csv.writer(buffer)
            writer.writerow([c.name for c in ItemData.__table__.columns])
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

            # Stream rows in chunks
            total = 0
            for row in query:
                writer.writerow([getattr(row, c.name) for c in ItemData.__table__.columns])
                total += 1

                if total % 100000 == 0:  # हर 1 लाख row पर भेजो
                    yield buffer.getvalue()
                    buffer.seek(0)
                    buffer.truncate(0)

            # बचे हुए rows भेजना ना भूले
            if buffer.tell() > 0:
                yield buffer.getvalue()
        finally:
            db.close()

    headers = {"Content-Disposition": "attachment; filename=incomplete_items.csv"}
    return Response(stream_with_context(generate()), mimetype="text/csv", headers=headers)
