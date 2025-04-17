from database import Base
from sqlalchemy.orm import Mapped, mapped_column


class Setting(Base):
    __tablename__ = 'settings'
    id: Mapped[int] = mapped_column(primary_key=True)

    purchase_index: Mapped[int] = mapped_column()
    sales_index: Mapped[int] = mapped_column()
    contra_index: Mapped[int] = mapped_column(nullable=True, default=None)

