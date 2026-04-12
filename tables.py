from typing import Optional

from sqlmodel import BigInteger, Column, Field, ForeignKey, Index, SQLModel


class UserTable(SQLModel, table=True):
    __tablename__ = "user"  # type: ignore
    uid: int = Field(sa_column=Column(BigInteger, primary_key=True))
    portrait: str
    current_profile_id: Optional[int] = Field(
        default=None,
        sa_column=Column(
            BigInteger,
            ForeignKey("user_profile.id", use_alter=True, ondelete="SET NULL"),
            nullable=True,
        ),
    )


class UserProfile(SQLModel, table=True):
    __tablename__ = "user_profile"  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)
    uid: int = Field(
        sa_column=Column(
            BigInteger, ForeignKey("user.uid", ondelete="CASCADE"), nullable=False
        )
    )
    changed_at: int
    username: str
    nickname: str
    portrait_id: int = Field(
        sa_column=Column(
            BigInteger, ForeignKey("image.id", ondelete="CASCADE"), nullable=False
        )
    )


class ForumTable(SQLModel, table=True):
    __tablename__ = "forum"  # type: ignore
    fid: int = Field(primary_key=True)
    forum_name: str


class ThreadTable(SQLModel, table=True):
    __tablename__ = "thread"  # type: ignore
    tid: int = Field(sa_column=Column(BigInteger, primary_key=True))
    author_id: int = Field(foreign_key="user.uid")
    title: str
    time: int
    updated_time: int
    forum_id: int = Field(foreign_key="forum.fid")
    content: str
    view_num: int
    reply_num: int
    share_num: int
    agree: int
    disagree: int


class PostTable(SQLModel, table=True):
    __tablename__ = "post"  # type: ignore
    pid: int = Field(sa_column=Column(BigInteger, primary_key=True))
    tid: int = Field(foreign_key="thread.tid")
    author_id: int = Field(foreign_key="user.uid")
    floor: int
    time: int
    content: str

    __table_args__ = (Index("idx_replies_thread_floor", "tid", "floor"),)


class CommentTable(SQLModel, table=True):
    __tablename__ = "comment"  # type: ignore
    cid: int = Field(sa_column=Column(BigInteger, primary_key=True))
    pid: int = Field(foreign_key="post.pid")
    author_id: int = Field(foreign_key="user.uid")
    time: int
    content: str
    reply_to: int = Field(foreign_key="user.uid")


class ImageTable(SQLModel, table=True):
    __tablename__ = "image"  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)
    tieba_hash: Optional[str] = Field()
    avatar_small_hash: Optional[str] = Field()
    avatar_large_hash: Optional[str] = Field()
