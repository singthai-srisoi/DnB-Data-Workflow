from database import Base
from sqlalchemy.orm import Mapped, mapped_column

class Customer(Base):
    __tablename__ = 'customers'
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(unique=True)
    control_ac: Mapped[str] = mapped_column()
    company_name: Mapped[str] = mapped_column()
    second_company_name: Mapped[str] = mapped_column(nullable=True, default="")
    address_1: Mapped[str] = mapped_column(nullable=True, default="")
    address_2: Mapped[str] = mapped_column(nullable=True, default="")
    address_3: Mapped[str] = mapped_column(nullable=True, default="")
    post_code: Mapped[str] = mapped_column(nullable=True, default="")
    tin: Mapped[str] = mapped_column(nullable=True, default="")
    id_type: Mapped[str] = mapped_column(nullable=True, default="")
    id_no: Mapped[str] = mapped_column(nullable=True, default="")