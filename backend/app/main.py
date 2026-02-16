from __future__ import annotations

import csv
import hashlib
import io
import itertools
import json
import re
import uuid
from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from itsdangerous import BadSignature, URLSafeSerializer
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, create_engine, select, text
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


DATABASE_URL = "sqlite:///./cmt.db"
SESSION_SECRET = "change-me"
serializer = URLSafeSerializer(SESSION_SECRET, salt="cmt")
DEFAULT_RESIDENCY_MIN_HOURS = 125.0
DEFAULT_RESIDENCY_MIN_ACADEMIC_SEMESTERS = 8
ALL_PLAN_PERIODS = tuple(range(0, 21))
ACADEMIC_PERIODS = (1, 2, 6, 7, 11, 12, 16, 17)
SUMMER_PERIODS = tuple(p for p in ALL_PLAN_PERIODS if p not in ACADEMIC_PERIODS)
MAX_PLAN_PERIOD = max(ALL_PLAN_PERIODS)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    username: Mapped[str] = mapped_column(String, unique=True, index=True)
    password: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String, default="DESIGN")


class AuditLog(Base):
    __tablename__ = "audit_log"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    actor_user_id: Mapped[str] = mapped_column(String)
    action: Mapped[str] = mapped_column(String)
    entity_type: Mapped[str] = mapped_column(String)
    entity_id: Mapped[str] = mapped_column(String)
    payload: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class CurriculumVersion(Base):
    __tablename__ = "curriculum_versions"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String, unique=True)
    status: Mapped[str] = mapped_column(String, default="DRAFT")
    parent_version_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("curriculum_versions.id"), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    effective_start_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    core_credit_hours: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    total_credit_hours: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Course(Base):
    __tablename__ = "courses"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)
    course_number: Mapped[str] = mapped_column(String, index=True)
    title: Mapped[str] = mapped_column(String)
    credit_hours: Mapped[float] = mapped_column(Float, default=3.0)
    designated_semester: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    offered_periods_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    min_section_size: Mapped[int] = mapped_column(Integer, default=6)


class AcademicProgram(Base):
    __tablename__ = "academic_programs"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)
    name: Mapped[str] = mapped_column(String)
    program_type: Mapped[str] = mapped_column(String, default="MAJOR")
    division: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class Requirement(Base):
    __tablename__ = "requirements"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)
    parent_requirement_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("requirements.id"), nullable=True)
    program_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("academic_programs.id"), nullable=True)
    name: Mapped[str] = mapped_column(String)
    logic_type: Mapped[str] = mapped_column(String, default="ALL_REQUIRED")
    pick_n: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    category: Mapped[str] = mapped_column(String, default="CORE")
    major_mode: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    track_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    option_slot_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    option_slot_capacity: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class PlanItem(Base):
    __tablename__ = "plan_items"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)
    semester_index: Mapped[int] = mapped_column(Integer)
    course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"))
    position: Mapped[int] = mapped_column(Integer, default=0)
    aspect: Mapped[str] = mapped_column(String, default="CORE")
    major_program_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("academic_programs.id"), nullable=True)
    track_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class DataBundleSnapshot(Base):
    __tablename__ = "data_bundle_snapshots"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)
    name: Mapped[str] = mapped_column(String)
    modules_csv: Mapped[str] = mapped_column(String, default="ALL")
    bundle_json: Mapped[str] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String, ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Instructor(Base):
    __tablename__ = "instructors"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String, index=True)
    department: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    max_sections_per_semester: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class InstructorQualification(Base):
    __tablename__ = "instructor_qualification"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    instructor_id: Mapped[str] = mapped_column(String, ForeignKey("instructors.id"), index=True)
    course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)


class Classroom(Base):
    __tablename__ = "classrooms"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    building: Mapped[str] = mapped_column(String)
    room_number: Mapped[str] = mapped_column(String)
    capacity: Mapped[int] = mapped_column(Integer)
    room_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class Section(Base):
    __tablename__ = "sections"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)
    course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    semester_label: Mapped[str] = mapped_column(String)
    max_enrollment: Mapped[int] = mapped_column(Integer, default=18)
    instructor_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("instructors.id"), nullable=True)
    classroom_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("classrooms.id"), nullable=True)


class Cadet(Base):
    __tablename__ = "cadets"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String, index=True)
    class_year: Mapped[int] = mapped_column(Integer, index=True)
    major_program_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("academic_programs.id"), nullable=True)
    cumulative_gpa: Mapped[float] = mapped_column(Float, default=0.0)


class CadetRecord(Base):
    __tablename__ = "cadet_records"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    cadet_id: Mapped[str] = mapped_column(String, ForeignKey("cadets.id"), index=True)
    course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    semester_label: Mapped[str] = mapped_column(String)
    grade: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    is_completed: Mapped[bool] = mapped_column(Boolean, default=False)


class RequirementFulfillment(Base):
    __tablename__ = "requirement_fulfillment"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    requirement_id: Mapped[str] = mapped_column(String, ForeignKey("requirements.id"), index=True)
    course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    required_semester: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    required_semester_min: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    required_semester_max: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class CohortAssignment(Base):
    __tablename__ = "cohort_assignments"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    class_year: Mapped[int] = mapped_column(Integer, index=True)
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)


class CourseEquivalency(Base):
    __tablename__ = "course_equivalency"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    from_version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"))
    to_version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"))
    from_course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"))
    to_course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"))


class CoursePrerequisite(Base):
    __tablename__ = "course_prerequisites"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    required_course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    relationship_type: Mapped[str] = mapped_column(String, default="PREREQUISITE")
    enforcement: Mapped[str] = mapped_column(String, default="HARD")


class CourseSubstitution(Base):
    __tablename__ = "course_substitutions"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    original_course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    substitute_course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    is_bidirectional: Mapped[bool] = mapped_column(Boolean, default=False)
    requires_approval: Mapped[bool] = mapped_column(Boolean, default=False)
    conditions_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class RequirementSubstitution(Base):
    __tablename__ = "requirement_substitutions"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    requirement_id: Mapped[str] = mapped_column(String, ForeignKey("requirements.id"), index=True)
    primary_course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    substitute_course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    is_bidirectional: Mapped[bool] = mapped_column(Boolean, default=False)


class CourseBucketTag(Base):
    __tablename__ = "course_bucket_tags"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    bucket_code: Mapped[str] = mapped_column(String, index=True)
    credit_hours_override: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)


class CourseBasket(Base):
    __tablename__ = "course_baskets"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)
    name: Mapped[str] = mapped_column(String)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)


class CourseBasketItem(Base):
    __tablename__ = "course_basket_items"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    basket_id: Mapped[str] = mapped_column(String, ForeignKey("course_baskets.id"), index=True)
    course_id: Mapped[str] = mapped_column(String, ForeignKey("courses.id"), index=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)


class RequirementBasketLink(Base):
    __tablename__ = "requirement_basket_links"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    requirement_id: Mapped[str] = mapped_column(String, ForeignKey("requirements.id"), index=True)
    basket_id: Mapped[str] = mapped_column(String, ForeignKey("course_baskets.id"), index=True)
    min_count: Mapped[int] = mapped_column(Integer, default=1)
    max_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)


class ValidationRule(Base):
    __tablename__ = "validation_rules"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String, unique=True)
    rule_code: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    tier: Mapped[int] = mapped_column(Integer, default=1)
    severity: Mapped[str] = mapped_column(String, default="WARNING")
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    config_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class DesignComment(Base):
    __tablename__ = "design_comments"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)
    entity_type: Mapped[str] = mapped_column(String)
    entity_id: Mapped[str] = mapped_column(String)
    comment: Mapped[str] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String, ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ChangeRequest(Base):
    __tablename__ = "change_requests"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)
    title: Mapped[str] = mapped_column(String)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String, default="PROPOSED")
    proposed_by: Mapped[str] = mapped_column(String, ForeignKey("users.id"))
    reviewed_by: Mapped[Optional[str]] = mapped_column(String, ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class CoiImportSession(Base):
    __tablename__ = "coi_import_sessions"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version_id: Mapped[str] = mapped_column(String, ForeignKey("curriculum_versions.id"), index=True)
    source_filename: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    min_course_number_occurrences: Mapped[int] = mapped_column(Integer, default=1)
    min_confidence: Mapped[float] = mapped_column(Float, default=0.4)
    status: Mapped[str] = mapped_column(String, default="DRAFT")
    created_by: Mapped[str] = mapped_column(String, ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class CoiImportItem(Base):
    __tablename__ = "coi_import_items"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    session_id: Mapped[str] = mapped_column(String, ForeignKey("coi_import_sessions.id"), index=True)
    course_number: Mapped[str] = mapped_column(String, index=True)
    title: Mapped[str] = mapped_column(String)
    edited_title: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    occurrences: Mapped[int] = mapped_column(Integer, default=1)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    include: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
app = FastAPI(title="USAFA CMT - Phases 1 and 2")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


class LoginIn(BaseModel):
    username: str
    password: str


class VersionIn(BaseModel):
    name: str
    description: Optional[str] = None
    effective_start_year: Optional[int] = None
    core_credit_hours: Optional[float] = None
    total_credit_hours: Optional[float] = None


class VersionOut(VersionIn):
    model_config = ConfigDict(from_attributes=True)
    id: str
    status: str
    parent_version_id: Optional[str] = None


class CourseIn(BaseModel):
    version_id: str
    course_number: str
    title: str
    credit_hours: float = 3.0
    designated_semester: Optional[int] = None
    offered_periods_json: Optional[str] = None
    min_section_size: int = 6


class ProgramIn(BaseModel):
    version_id: str
    name: str
    program_type: str = "MAJOR"
    division: Optional[str] = None


class RequirementIn(BaseModel):
    version_id: str
    name: str
    program_id: Optional[str] = None
    parent_requirement_id: Optional[str] = None
    logic_type: str = "ALL_REQUIRED"
    pick_n: Optional[int] = None
    sort_order: Optional[int] = None
    category: str = "CORE"
    major_mode: Optional[str] = None
    track_name: Optional[str] = None
    option_slot_key: Optional[str] = None
    option_slot_capacity: Optional[int] = Field(default=None, ge=1, le=20)


class InstructorIn(BaseModel):
    name: str
    department: Optional[str] = None
    max_sections_per_semester: Optional[int] = None


class ClassroomIn(BaseModel):
    building: str
    room_number: str
    capacity: int
    room_type: Optional[str] = None


class SectionIn(BaseModel):
    version_id: str
    course_id: str
    semester_label: str
    max_enrollment: int = 18
    instructor_id: Optional[str] = None
    classroom_id: Optional[str] = None


class CadetIn(BaseModel):
    name: str
    class_year: int
    major_program_id: Optional[str] = None
    cumulative_gpa: float = 0.0


class CadetRecordIn(BaseModel):
    cadet_id: str
    course_id: str
    semester_label: str
    grade: Optional[str] = None
    is_completed: bool = False


class RequirementFulfillmentIn(BaseModel):
    requirement_id: str
    course_id: str
    is_primary: bool = False
    required_semester: Optional[int] = Field(default=None, ge=0, le=MAX_PLAN_PERIOD)
    required_semester_min: Optional[int] = Field(default=None, ge=0, le=MAX_PLAN_PERIOD)
    required_semester_max: Optional[int] = Field(default=None, ge=0, le=MAX_PLAN_PERIOD)


class RequirementSubstitutionIn(BaseModel):
    requirement_id: str
    primary_course_id: str
    substitute_course_id: str
    is_bidirectional: bool = False


class MoveIn(BaseModel):
    plan_item_id: str
    target_semester: int = Field(ge=0, le=MAX_PLAN_PERIOD)
    target_position: int = Field(ge=0)


class CanvasItemUpdateIn(BaseModel):
    semester_index: Optional[int] = Field(default=None, ge=0, le=MAX_PLAN_PERIOD)
    category: Optional[str] = None
    major_mode: Optional[str] = None
    major_program_id: Optional[str] = None
    track_name: Optional[str] = None
    aspect: Optional[str] = None


class CanvasSequenceItemIn(BaseModel):
    semester_index: int = Field(ge=0, le=MAX_PLAN_PERIOD)
    course_id: Optional[str] = None
    course_number: Optional[str] = None
    position: Optional[int] = None
    aspect: Optional[str] = None
    major_program_id: Optional[str] = None
    major_program_name: Optional[str] = None
    track_name: Optional[str] = None


class CanvasSequenceImportIn(BaseModel):
    name: Optional[str] = None
    replace_existing: bool = True
    items: list[CanvasSequenceItemIn] = Field(default_factory=list)


class DataBundleImportIn(BaseModel):
    bundle: dict = Field(default_factory=dict)
    modules: Optional[list[str]] = None
    replace_existing: bool = True


class PrerequisiteIn(BaseModel):
    course_id: str
    required_course_id: str
    relationship_type: str = "PREREQUISITE"
    enforcement: str = "HARD"


class SubstitutionIn(BaseModel):
    original_course_id: str
    substitute_course_id: str
    is_bidirectional: bool = False
    requires_approval: bool = False
    conditions: Optional[dict] = None


class CourseBucketTagIn(BaseModel):
    bucket_code: str
    credit_hours_override: Optional[float] = Field(default=None, ge=0.0)
    sort_order: int = Field(default=0, ge=0)


class ValidationRuleIn(BaseModel):
    rule_code: Optional[str] = None
    name: str
    tier: int = Field(ge=1, le=3)
    severity: str = "WARNING"
    active: bool = True
    config: dict = Field(default_factory=dict)


class ValidationRuleUpdateIn(BaseModel):
    rule_code: Optional[str] = None
    name: Optional[str] = None
    tier: Optional[int] = Field(default=None, ge=1, le=3)
    severity: Optional[str] = None
    active: Optional[bool] = None
    config: Optional[dict] = None


class RequirementOrderIn(BaseModel):
    requirement_id: str
    sort_order: int = Field(ge=0)


class RequirementTreeNodeIn(BaseModel):
    requirement_id: str
    parent_requirement_id: Optional[str] = None
    sort_order: int = Field(ge=0)


class CourseBasketIn(BaseModel):
    version_id: str
    name: str
    description: Optional[str] = None
    sort_order: Optional[int] = Field(default=None, ge=0)


class CourseBasketItemIn(BaseModel):
    basket_id: str
    course_id: str
    sort_order: Optional[int] = Field(default=None, ge=0)


class RequirementBasketLinkIn(BaseModel):
    requirement_id: str
    basket_id: str
    min_count: int = Field(default=1, ge=1, le=50)
    max_count: Optional[int] = Field(default=None, ge=1, le=50)
    sort_order: Optional[int] = Field(default=None, ge=0)


class CourseBasketItemOrderIn(BaseModel):
    item_id: str
    basket_id: Optional[str] = None
    sort_order: int = Field(ge=0)


class RequirementBasketOrderIn(BaseModel):
    link_id: str
    requirement_id: Optional[str] = None
    sort_order: int = Field(ge=0)


class RequirementFulfillmentOrderIn(BaseModel):
    fulfillment_id: str
    sort_order: int = Field(ge=0)
    requirement_id: Optional[str] = None


class DesignCommentIn(BaseModel):
    version_id: str
    entity_type: str
    entity_id: str
    comment: str


class ChangeRequestIn(BaseModel):
    version_id: str
    title: str
    description: Optional[str] = None


class CoiLoadOptions(BaseModel):
    version_id: str
    replace_existing: bool = False
    default_credit_hours: float = 3.0
    min_course_number_occurrences: int = 1
    min_confidence: float = 0.4


class CoiReviewDecision(BaseModel):
    item_id: str
    include: bool
    edited_title: Optional[str] = None


class CoiReviewDecisionsIn(BaseModel):
    decisions: list[CoiReviewDecision]


class CoiReviewCommitOptions(BaseModel):
    replace_existing: bool = False
    default_credit_hours: float = 3.0


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def current_user(session_token: str = Query(...), db: Session = Depends(get_db)) -> User:
    try:
        payload = serializer.loads(session_token)
    except BadSignature as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc
    user = db.get(User, payload["user_id"])
    if not user:
        raise HTTPException(status_code=401, detail="Invalid user")
    return user


def require_design(user: User = Depends(current_user)) -> User:
    if user.role != "DESIGN":
        raise HTTPException(status_code=403, detail="DESIGN role required")
    return user


def write_audit(db: Session, user: User, action: str, entity: str, entity_id: str, payload: Optional[str] = None) -> None:
    db.add(AuditLog(actor_user_id=user.id, action=action, entity_type=entity, entity_id=entity_id, payload=payload))
    db.commit()


def serialize(instance):
    return {c.key: getattr(instance, c.key) for c in inspect(instance).mapper.column_attrs}


def normalize_course_number(raw: str) -> str:
    s = re.sub(r"\s+", "", raw.upper())
    m = re.match(r"^([A-Z]{2,10})(\d{3}[A-Z]?)$", s)
    if not m:
        return re.sub(r"\s+", " ", raw.strip())
    return f"{m.group(1)} {m.group(2)}"


def period_label(period_index: int) -> str:
    if period_index == 0:
        return "Summer 0"
    semester_map = {
        1: "Semester 1",
        2: "Semester 2",
        6: "Semester 3",
        7: "Semester 4",
        11: "Semester 5",
        12: "Semester 6",
        16: "Semester 7",
        17: "Semester 8",
    }
    if period_index in semester_map:
        return semester_map[period_index]
    summer_period_map = {
        3: "Summer 1 Period 1",
        4: "Summer 1 Period 2",
        5: "Summer 1 Period 3",
        8: "Summer 2 Period 1",
        9: "Summer 2 Period 2",
        10: "Summer 2 Period 3",
        13: "Summer 3 Period 1",
        14: "Summer 3 Period 2",
        15: "Summer 3 Period 3",
        18: "Summer 4 Period 1",
        19: "Summer 4 Period 2",
        20: "Summer 4 Period 3",
    }
    if period_index in summer_period_map:
        return summer_period_map[period_index]
    return f"Period {period_index}"


def period_short_label(period_index: int) -> str:
    short_map = {
        0: "SU0",
        1: "S1",
        2: "S2",
        3: "SU1P1",
        4: "SU1P2",
        5: "SU1P3",
        6: "S3",
        7: "S4",
        8: "SU2P1",
        9: "SU2P2",
        10: "SU2P3",
        11: "S5",
        12: "S6",
        13: "SU3P1",
        14: "SU3P2",
        15: "SU3P3",
        16: "S7",
        17: "S8",
        18: "SU4P1",
        19: "SU4P2",
        20: "SU4P3",
    }
    if period_index in short_map:
        return short_map[period_index]
    return f"P{period_index}"


def period_kind(period_index: int) -> str:
    return "SUMMER" if period_index in SUMMER_PERIODS else "ACADEMIC"


def list_period_metadata() -> list[dict]:
    layout_position = {
        16: {"row": 0, "col": 0},
        17: {"row": 0, "col": 1},
        18: {"row": 0, "col": 2},
        19: {"row": 0, "col": 3},
        20: {"row": 0, "col": 4},
        11: {"row": 1, "col": 0},
        12: {"row": 1, "col": 1},
        13: {"row": 1, "col": 2},
        14: {"row": 1, "col": 3},
        15: {"row": 1, "col": 4},
        6: {"row": 2, "col": 0},
        7: {"row": 2, "col": 1},
        8: {"row": 2, "col": 2},
        9: {"row": 2, "col": 3},
        10: {"row": 2, "col": 4},
        1: {"row": 3, "col": 0},
        2: {"row": 3, "col": 1},
        3: {"row": 3, "col": 2},
        4: {"row": 3, "col": 3},
        5: {"row": 3, "col": 4},
        0: {"row": 4, "col": 4},
    }
    return [
        {
            "index": i,
            "short_label": period_short_label(i),
            "label": period_label(i),
            "kind": period_kind(i),
            "layout_row": layout_position.get(i, {}).get("row"),
            "layout_col": layout_position.get(i, {}).get("col"),
        }
        for i in ALL_PLAN_PERIODS
    ]


def semester_constraint_allows(sem: int, required_semester: Optional[int], required_semester_min: Optional[int], required_semester_max: Optional[int]) -> bool:
    if required_semester is not None and sem != required_semester:
        return False
    if required_semester_min is not None and sem < required_semester_min:
        return False
    if required_semester_max is not None and sem > required_semester_max:
        return False
    return True


def timing_constraints_overlap(
    a_required: Optional[int],
    a_min: Optional[int],
    a_max: Optional[int],
    b_required: Optional[int],
    b_min: Optional[int],
    b_max: Optional[int],
) -> bool:
    for s in ALL_PLAN_PERIODS:
        if semester_constraint_allows(s, a_required, a_min, a_max) and semester_constraint_allows(s, b_required, b_min, b_max):
            return True
    return False


@app.get("/meta/periods")
def period_metadata(_: User = Depends(current_user)):
    return {"periods": list_period_metadata()}


def validate_core_pathway_rule_config(cfg: dict, db: Session) -> None:
    rule_type = str(cfg.get("type") or "").upper()
    if rule_type not in {"MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"}:
        return
    program_id = cfg.get("program_id")
    program_name = str(cfg.get("program_name") or "").strip()
    target_program = None
    if program_id:
        target_program = db.get(AcademicProgram, program_id)
    elif program_name:
        target_program = db.scalar(select(AcademicProgram).where(AcademicProgram.name == program_name))
    if not target_program:
        raise HTTPException(status_code=400, detail="Core Rules target program not found")
    version_id = target_program.version_id
    # Core pathway timing must be compatible with core timing constraints, not other majors/minors.
    req_ids = [
        r.id
        for r in db.scalars(
            select(Requirement).where(
                Requirement.version_id == version_id,
                Requirement.category == "CORE",
                Requirement.program_id.is_(None),
            )
        ).all()
    ]
    rf_rows = (
        db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_(req_ids))).all()
        if req_ids
        else []
    )
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    course_num_by_id = {c.id: normalize_course_number(c.course_number) for c in courses if c.course_number}
    existing_by_course_num: dict[str, list[tuple[Optional[int], Optional[int], Optional[int]]]] = {}
    for rf in rf_rows:
        if rf.required_semester is None and rf.required_semester_min is None and rf.required_semester_max is None:
            continue
        cnum = course_num_by_id.get(rf.course_id)
        if not cnum:
            continue
        existing_by_course_num.setdefault(cnum, []).append((rf.required_semester, rf.required_semester_min, rf.required_semester_max))

    for idx, group in enumerate(cfg.get("required_core_groups") or []):
        group = group or {}
        rs = group.get("required_semester")
        rs_min = group.get("required_semester_min")
        rs_max = group.get("required_semester_max")
        try:
            rs = int(rs) if rs is not None else None
        except Exception:
            rs = None
        try:
            rs_min = int(rs_min) if rs_min is not None else None
        except Exception:
            rs_min = None
        try:
            rs_max = int(rs_max) if rs_max is not None else None
        except Exception:
            rs_max = None
        if rs is not None and (rs_min is not None or rs_max is not None):
            raise HTTPException(status_code=400, detail=f"Core Rules group {idx + 1}: fixed semester cannot be combined with range")
        if rs_min is not None and rs_max is not None and rs_min > rs_max:
            raise HTTPException(status_code=400, detail=f"Core Rules group {idx + 1}: no-earlier cannot be later than no-later")
        candidate_nums = [normalize_course_number(str(x)) for x in (group.get("course_numbers") or []) if str(x).strip()]
        if rs is None and rs_min is None and rs_max is None:
            continue
        for num in candidate_nums:
            for ex_rs, ex_min, ex_max in existing_by_course_num.get(num, []):
                if not timing_constraints_overlap(rs, rs_min, rs_max, ex_rs, ex_min, ex_max):
                    raise HTTPException(
                        status_code=400,
                        detail=f"Core Rules timing conflicts with existing requirement timing for {num}",
                    )


def normalize_division(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip().upper().replace("&", "AND").replace("-", "_").replace(" ", "_")
    if not s:
        return None
    aliases = {
        "SOCIAL_SCIENCE": "SOCIAL_SCIENCES",
        "BASIC_SCIENCE_AND_MATH": "BASIC_SCIENCES_AND_MATH",
        "BASIC_SCIENCES_MATH": "BASIC_SCIENCES_AND_MATH",
        "ENGINEERING_SCIENCE": "ENGINEERING_SCIENCES",
    }
    s = aliases.get(s, s)
    allowed = {"SOCIAL_SCIENCES", "HUMANITIES", "BASIC_SCIENCES_AND_MATH", "ENGINEERING_SCIENCES"}
    if s not in allowed:
        raise HTTPException(status_code=400, detail="division must be one of SOCIAL_SCIENCES, HUMANITIES, BASIC_SCIENCES_AND_MATH, ENGINEERING_SCIENCES")
    return s


def infer_division_from_program_name(name: str) -> Optional[str]:
    n = str(name or "").strip().lower()
    if not n:
        return None
    if any(k in n for k in ["aero", "astro", "electrical", "mechanical", "civil", "systems", "computer science", "cyber"]):
        return "ENGINEERING_SCIENCES"
    if any(k in n for k in ["math", "physics", "chem", "biology", "biochem", "geology"]):
        return "BASIC_SCIENCES_AND_MATH"
    if any(k in n for k in ["history", "politic", "econom", "law", "behavior", "psych", "sociolog"]):
        return "SOCIAL_SCIENCES"
    if any(k in n for k in ["english", "philosophy", "language", "literature", "relig", "foreign area"]):
        return "HUMANITIES"
    return None


def infer_title_from_text(text: str, start_idx: int) -> str:
    tail = text[start_idx : start_idx + 180]
    tail = tail.replace("\r", " ").replace("\n", " ")
    tail = re.sub(r"\s+", " ", tail).strip(" -:\t")
    # Stop early on common delimiters.
    for sep in ["  ", ";", "|"]:
        if sep in tail:
            tail = tail.split(sep)[0].strip()
    # Remove credit/hour patterns from title.
    tail = re.sub(r"\b\d+(?:\.\d+)?\s*\(\d+\)\b", "", tail).strip(" -:")
    # Keep a reasonable title window.
    words = tail.split()
    title = " ".join(words[:12]).strip()
    if not title:
        return "TBD Title"
    return title


def parse_coi_courses(text: str, min_occurrences: int = 1) -> dict[str, dict]:
    work_text = text
    upper = text.upper()
    start_idx = upper.rfind("APPENDIX 2: COURSE DESCRIPTIONS")
    if start_idx >= 0:
        end_idx = upper.find("APPENDIX 3: DEFINITION OF TERMS", start_idx)
        if end_idx > start_idx:
            work_text = text[start_idx:end_idx]
        else:
            work_text = text[start_idx:]

    parsed: dict[str, dict] = {}
    key_to_display: dict[str, str] = {}

    def upsert(course_number: str, title: str, occurrences: int, confidence: float, source: str) -> None:
        key = re.sub(r"\s+", "", course_number.upper())
        if key not in parsed:
            key_to_display[key] = course_number
            parsed[key] = {
                "course_number": course_number,
                "title": title,
                "occurrences": occurrences,
                "confidence": confidence,
                "source": source,
            }
            return
        existing = parsed[key]
        # Prefer higher confidence, then longer non-empty title.
        if confidence > existing["confidence"] or (confidence == existing["confidence"] and len(title) > len(existing["title"])):
            parsed[key] = {
                "course_number": course_number,
                "title": title,
                "occurrences": max(existing["occurrences"], occurrences),
                "confidence": confidence,
                "source": source,
            }
        else:
            existing["occurrences"] = max(existing["occurrences"], occurrences)

    # Pattern A: explicit course entry style, e.g. "Mil Tng 220. Combat Survival Training ..."
    entry_pattern = re.compile(
        r"\b([A-Z][A-Za-z]{1,10}(?:\s+[A-Z][A-Za-z]{1,10})?)\s+([1-7]\d{2}[A-Z]?)\.\s{2,}([A-Z][^.\r\n]{2,140})"
    )
    stop_prefixes = {"TABLE", "FIGURE", "CHAPTER", "SECTION", "APPENDIX", "INDEX", "LIST"}
    for m in entry_pattern.finditer(work_text):
        prefix = re.sub(r"\s+", " ", m.group(1).strip())
        if prefix.upper() in stop_prefixes:
            continue
        num = m.group(2).strip()
        course_number = f"{prefix} {num}"
        title = re.sub(r"\s+", " ", m.group(3).strip(" -:\t"))
        upsert(course_number, title, 1, 0.95, "entry_pattern")

    # Pattern B: canonical uppercase code references (fallback), e.g. "CS 101"
    ref_pattern = re.compile(r"\b([A-Z]{2,5}\s?\d{3}[A-Z]?)\b")
    matches = list(ref_pattern.finditer(work_text))
    freq: dict[str, int] = {}
    first_pos: dict[str, int] = {}
    for m in matches:
        code = normalize_course_number(m.group(1))
        # Filter obvious non-course noise fragments.
        if re.match(r"^[A-Z]\s+\d{3}$", code):
            continue
        freq[code] = freq.get(code, 0) + 1
        if code not in first_pos:
            first_pos[code] = m.end()
    for code, count in freq.items():
        if count < min_occurrences:
            continue
        title = infer_title_from_text(work_text, first_pos[code])
        conf = 0.6 if count >= 2 else 0.45
        if title == "TBD Title":
            conf -= 0.15
        upsert(code, title, count, max(0.1, conf), "reference_pattern")

    # Return keyed by display course number for stable downstream behavior.
    out: dict[str, dict] = {}
    for key, record in parsed.items():
        out[key_to_display[key]] = record
    return out


def map_legacy_period_index(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    mapping = {
        1: 1,
        2: 2,
        3: 6,
        4: 7,
        5: 11,
        6: 12,
        7: 16,
        8: 17,
        9: 3,
        10: 8,
        11: 13,
    }
    return mapping.get(value, value)


def ensure_runtime_migrations() -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS runtime_flags (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
        )
        cols = conn.execute(text("PRAGMA table_info(requirements)")).fetchall()
        col_names = {c[1] for c in cols}
        if "sort_order" not in col_names:
            conn.execute(text("ALTER TABLE requirements ADD COLUMN sort_order INTEGER DEFAULT 0"))
        if "category" not in col_names:
            conn.execute(text("ALTER TABLE requirements ADD COLUMN category TEXT DEFAULT 'CORE'"))
        if "major_mode" not in col_names:
            conn.execute(text("ALTER TABLE requirements ADD COLUMN major_mode TEXT"))
        if "track_name" not in col_names:
            conn.execute(text("ALTER TABLE requirements ADD COLUMN track_name TEXT"))
        if "option_slot_key" not in col_names:
            conn.execute(text("ALTER TABLE requirements ADD COLUMN option_slot_key TEXT"))
        if "option_slot_capacity" not in col_names:
            conn.execute(text("ALTER TABLE requirements ADD COLUMN option_slot_capacity INTEGER"))
        plan_cols = conn.execute(text("PRAGMA table_info(plan_items)")).fetchall()
        plan_col_names = {c[1] for c in plan_cols}
        if "aspect" not in plan_col_names:
            conn.execute(text("ALTER TABLE plan_items ADD COLUMN aspect TEXT DEFAULT 'CORE'"))
        if "major_program_id" not in plan_col_names:
            conn.execute(text("ALTER TABLE plan_items ADD COLUMN major_program_id TEXT"))
        if "track_name" not in plan_col_names:
            conn.execute(text("ALTER TABLE plan_items ADD COLUMN track_name TEXT"))
        course_cols = conn.execute(text("PRAGMA table_info(courses)")).fetchall()
        course_col_names = {c[1] for c in course_cols}
        if "offered_periods_json" not in course_col_names:
            conn.execute(text("ALTER TABLE courses ADD COLUMN offered_periods_json TEXT"))
        rf_cols = conn.execute(text("PRAGMA table_info(requirement_fulfillment)")).fetchall()
        rf_col_names = {c[1] for c in rf_cols}
        if "sort_order" not in rf_col_names:
            conn.execute(text("ALTER TABLE requirement_fulfillment ADD COLUMN sort_order INTEGER DEFAULT 0"))
        if "required_semester" not in rf_col_names:
            conn.execute(text("ALTER TABLE requirement_fulfillment ADD COLUMN required_semester INTEGER"))
        if "required_semester_min" not in rf_col_names:
            conn.execute(text("ALTER TABLE requirement_fulfillment ADD COLUMN required_semester_min INTEGER"))
        if "required_semester_max" not in rf_col_names:
            conn.execute(text("ALTER TABLE requirement_fulfillment ADD COLUMN required_semester_max INTEGER"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS requirement_substitutions (
                    id TEXT PRIMARY KEY,
                    requirement_id TEXT NOT NULL,
                    primary_course_id TEXT NOT NULL,
                    substitute_course_id TEXT NOT NULL,
                    is_bidirectional BOOLEAN DEFAULT 0
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS course_bucket_tags (
                    id TEXT PRIMARY KEY,
                    course_id TEXT NOT NULL,
                    bucket_code TEXT NOT NULL,
                    credit_hours_override REAL,
                    sort_order INTEGER DEFAULT 0
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS course_baskets (
                    id TEXT PRIMARY KEY,
                    version_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    sort_order INTEGER DEFAULT 0
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS course_basket_items (
                    id TEXT PRIMARY KEY,
                    basket_id TEXT NOT NULL,
                    course_id TEXT NOT NULL,
                    sort_order INTEGER DEFAULT 0
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS requirement_basket_links (
                    id TEXT PRIMARY KEY,
                    requirement_id TEXT NOT NULL,
                    basket_id TEXT NOT NULL,
                    min_count INTEGER DEFAULT 1,
                    max_count INTEGER,
                    sort_order INTEGER DEFAULT 0
                )
                """
            )
        )
        prog_cols = conn.execute(text("PRAGMA table_info(academic_programs)")).fetchall()
        prog_col_names = {c[1] for c in prog_cols}
        if "division" not in prog_col_names:
            conn.execute(text("ALTER TABLE academic_programs ADD COLUMN division TEXT"))
        vr_cols = conn.execute(text("PRAGMA table_info(validation_rules)")).fetchall()
        vr_col_names = {c[1] for c in vr_cols}
        if "rule_code" not in vr_col_names:
            conn.execute(text("ALTER TABLE validation_rules ADD COLUMN rule_code TEXT"))

        migrated = conn.execute(
            text("SELECT value FROM runtime_flags WHERE key = 'period_model_v2_migrated'")
        ).fetchone()
        if not migrated:
            conn.execute(
                text(
                    """
                    UPDATE plan_items
                    SET semester_index = CASE semester_index
                        WHEN 1 THEN 1
                        WHEN 2 THEN 2
                        WHEN 3 THEN 6
                        WHEN 4 THEN 7
                        WHEN 5 THEN 11
                        WHEN 6 THEN 12
                        WHEN 7 THEN 16
                        WHEN 8 THEN 17
                        WHEN 9 THEN 3
                        WHEN 10 THEN 8
                        WHEN 11 THEN 13
                        ELSE semester_index
                    END
                    WHERE semester_index BETWEEN 1 AND 11
                    """
                )
            )
            conn.execute(
                text(
                    """
                    UPDATE courses
                    SET designated_semester = CASE designated_semester
                        WHEN 1 THEN 1
                        WHEN 2 THEN 2
                        WHEN 3 THEN 6
                        WHEN 4 THEN 7
                        WHEN 5 THEN 11
                        WHEN 6 THEN 12
                        WHEN 7 THEN 16
                        WHEN 8 THEN 17
                        WHEN 9 THEN 3
                        WHEN 10 THEN 8
                        WHEN 11 THEN 13
                        ELSE designated_semester
                    END
                    WHERE designated_semester BETWEEN 1 AND 11
                    """
                )
            )
            conn.execute(
                text(
                    """
                    UPDATE requirement_fulfillment
                    SET required_semester = CASE required_semester
                        WHEN 1 THEN 1
                        WHEN 2 THEN 2
                        WHEN 3 THEN 6
                        WHEN 4 THEN 7
                        WHEN 5 THEN 11
                        WHEN 6 THEN 12
                        WHEN 7 THEN 16
                        WHEN 8 THEN 17
                        WHEN 9 THEN 3
                        WHEN 10 THEN 8
                        WHEN 11 THEN 13
                        ELSE required_semester
                    END
                    WHERE required_semester BETWEEN 1 AND 11
                    """
                )
            )
            conn.execute(
                text(
                    """
                    UPDATE requirement_fulfillment
                    SET required_semester_min = CASE required_semester_min
                        WHEN 1 THEN 1
                        WHEN 2 THEN 2
                        WHEN 3 THEN 6
                        WHEN 4 THEN 7
                        WHEN 5 THEN 11
                        WHEN 6 THEN 12
                        WHEN 7 THEN 16
                        WHEN 8 THEN 17
                        WHEN 9 THEN 3
                        WHEN 10 THEN 8
                        WHEN 11 THEN 13
                        ELSE required_semester_min
                    END
                    WHERE required_semester_min BETWEEN 1 AND 11
                    """
                )
            )
            conn.execute(
                text(
                    """
                    UPDATE requirement_fulfillment
                    SET required_semester_max = CASE required_semester_max
                        WHEN 1 THEN 1
                        WHEN 2 THEN 2
                        WHEN 3 THEN 6
                        WHEN 4 THEN 7
                        WHEN 5 THEN 11
                        WHEN 6 THEN 12
                        WHEN 7 THEN 16
                        WHEN 8 THEN 17
                        WHEN 9 THEN 3
                        WHEN 10 THEN 8
                        WHEN 11 THEN 13
                        ELSE required_semester_max
                    END
                    WHERE required_semester_max BETWEEN 1 AND 11
                    """
                )
            )
            conn.execute(text("INSERT INTO runtime_flags(key, value) VALUES ('period_model_v2_migrated', '1')"))


def ensure_period_config_migration(db: Session) -> None:
    done = db.execute(text("SELECT value FROM runtime_flags WHERE key = 'period_rule_config_v2_migrated'")).first()
    if done:
        return
    rules = db.scalars(select(ValidationRule)).all()
    for rule in rules:
        try:
            cfg = json.loads(rule.config_json or "{}")
        except Exception:
            continue
        changed = False
        groups = cfg.get("required_core_groups")
        if isinstance(groups, list):
            for g in groups:
                if not isinstance(g, dict):
                    continue
                for key in ("required_semester", "required_semester_min", "required_semester_max"):
                    raw = g.get(key)
                    if raw is None:
                        continue
                    try:
                        original = int(raw)
                    except Exception:
                        continue
                    mapped = map_legacy_period_index(original)
                    if mapped != original:
                        g[key] = mapped
                        changed = True
        if changed:
            rule.config_json = json.dumps(cfg)
    db.execute(text("INSERT INTO runtime_flags(key, value) VALUES ('period_rule_config_v2_migrated', '1')"))
    db.commit()


def next_validation_rule_code(db: Session) -> str:
    max_n = 0
    for r in db.scalars(select(ValidationRule)).all():
        m = re.match(r"^R(\d+)$", str(r.rule_code or "").strip().upper())
        if m:
            max_n = max(max_n, int(m.group(1)))
    return f"R{max_n + 1}"


def ensure_validation_rule_codes(db: Session) -> None:
    changed = False
    for r in db.scalars(select(ValidationRule).order_by(ValidationRule.name.asc())).all():
        if str(r.rule_code or "").strip():
            continue
        r.rule_code = next_validation_rule_code(db)
        changed = True
    if changed:
        db.commit()


def active_rules_with_config(db: Session) -> list[tuple[ValidationRule, dict]]:
    rows: list[tuple[ValidationRule, dict]] = []
    for r in db.scalars(select(ValidationRule).where(ValidationRule.active.is_(True))).all():
        try:
            cfg = json.loads(r.config_json or "{}")
        except Exception:
            cfg = {}
        rows.append((r, cfg if isinstance(cfg, dict) else {}))
    return rows


def rule_domain(cfg: dict) -> str:
    return str((cfg or {}).get("domain") or "").strip()


def is_cadet_performance_rule(cfg: dict) -> bool:
    return rule_domain(cfg).lower() == "cadet performance"


def is_definitional_rule(cfg: dict) -> bool:
    return rule_domain(cfg).lower() == "definitional"


def active_design_rules_with_config(db: Session) -> list[tuple[ValidationRule, dict]]:
    return [
        (rule, cfg)
        for rule, cfg in active_rules_with_config(db)
        if not is_cadet_performance_rule(cfg) and not is_definitional_rule(cfg)
    ]


def rule_targets_programs(cfg: dict, programs: list[AcademicProgram]) -> bool:
    if not programs:
        return False
    target_ids = set()
    target_names = set()
    if cfg.get("program_id"):
        target_ids.add(str(cfg.get("program_id")))
    for x in (cfg.get("program_ids") or []):
        target_ids.add(str(x))
    if cfg.get("program_name"):
        target_names.add(str(cfg.get("program_name")).strip().lower())
    for x in (cfg.get("program_names") or []):
        target_names.add(str(x).strip().lower())
    if not target_ids and not target_names:
        return True
    for p in programs:
        if p.id in target_ids:
            return True
        if str(p.name or "").strip().lower() in target_names:
            return True
    return False


def bucket_credits_for_course_ids(
    course_ids: set[str],
    bucket_rows: list[CourseBucketTag],
    course_by_id: dict[str, Course],
    bucket_code: str,
) -> float:
    code = str(bucket_code or "").strip().upper()
    if not code or not course_ids:
        return 0.0
    total = 0.0
    for row in bucket_rows:
        if row.course_id not in course_ids:
            continue
        if str(row.bucket_code or "").strip().upper() != code:
            continue
        if row.credit_hours_override is not None:
            total += float(row.credit_hours_override)
        else:
            c = course_by_id.get(row.course_id)
            total += float(c.credit_hours if c else 0.0)
    return total


def abet_validation_items_for_courses(
    rule_rows: list[tuple[ValidationRule, dict]],
    *,
    programs: list[AcademicProgram],
    course_ids: set[str],
    bucket_rows: list[CourseBucketTag],
    course_by_id: dict[str, Course],
) -> list[dict]:
    out: list[dict] = []
    if not programs or not course_ids:
        return out
    for rule, cfg in rule_rows:
        rtype = str(cfg.get("type") or "").upper().strip()
        if rtype not in {"ABET_EAC_MATH_BASIC_SCI_MIN", "ABET_EAC_ENGINEERING_TOPICS_MIN"}:
            continue
        if not rule_targets_programs(cfg, programs):
            continue
        default_bucket = "ABET_MATH_BASIC_SCI" if rtype == "ABET_EAC_MATH_BASIC_SCI_MIN" else "ABET_ENGINEERING_TOPICS"
        bucket_code = str(cfg.get("bucket_code") or default_bucket).strip().upper()
        try:
            min_credits = float(cfg.get("min_credits", 30.0 if rtype == "ABET_EAC_MATH_BASIC_SCI_MIN" else 45.0))
        except Exception:
            min_credits = 30.0 if rtype == "ABET_EAC_MATH_BASIC_SCI_MIN" else 45.0
        actual = bucket_credits_for_course_ids(course_ids, bucket_rows, course_by_id, bucket_code)
        out.append(
            {
                "rule_code": str(rule.rule_code or "").strip(),
                "rule_name": rule.name,
                "status": ("PASS" if actual >= min_credits else "FAIL"),
                "message": f"{bucket_code} credits {actual:.1f}; minimum {min_credits:.1f}.",
            }
        )
    return out


def get_pathway_definitions(rule_rows: list[tuple[ValidationRule, dict]]) -> dict:
    upper_level_min = 300

    _def_upper_rule, def_upper_cfg = find_active_rule(
        rule_rows,
        rule_type="DEF_UPPER_LEVEL_COURSE_NUMBER",
        names=["Program/Major Pathway Definition: Upper-level course number minimum"],
    )

    try:
        upper_level_min = int(def_upper_cfg.get("min_level", upper_level_min))
    except Exception:
        upper_level_min = 300

    return {
        "upper_level_min": upper_level_min,
        "double_major_additional_hours_basis": "UNION_MINUS_LARGER_MAJOR",
    }


def build_pathway_validation_items(
    *,
    rule_rows: list[tuple[ValidationRule, dict]],
    programs: list[AcademicProgram],
    program_course_sets: dict[str, set[str]],
    program_credit_sums: dict[str, float],
    course_by_id: dict[str, Course],
    definitions: dict,
) -> list[dict]:
    out: list[dict] = []
    selected_majors = [p for p in programs if (p.program_type or "").upper() == "MAJOR"]
    selected_minors = [p for p in programs if (p.program_type or "").upper() == "MINOR"]
    upper_min = int(definitions.get("upper_level_min", 300))

    minor_courses_rule, minor_courses_cfg = find_active_rule(
        rule_rows,
        rule_type="MINOR_MIN_COURSES",
        names=["Program/Major Pathway: Minor minimum courses"],
    )
    minor_hours_rule, minor_hours_cfg = find_active_rule(
        rule_rows,
        rule_type="MINOR_MIN_HOURS",
        names=["Program/Major Pathway: Minor minimum hours"],
    )
    minor_upper_rule, minor_upper_cfg = find_active_rule(
        rule_rows,
        rule_type="MINOR_MIN_UPPER_LEVEL_COURSES",
        names=["Program/Major Pathway: Minor upper-level courses minimum"],
    )
    dm_div_rule, _dm_div_cfg = find_active_rule(
        rule_rows,
        rule_type="DOUBLE_MAJOR_DIVISION_SEPARATION",
        names=["Program/Major Pathway: Double major divisional separation"],
    )
    dm_add_rule, dm_add_cfg = find_active_rule(
        rule_rows,
        rule_type="DOUBLE_MAJOR_ADDITIONAL_HOURS_MIN",
        names=["Program/Major Pathway: Double major additional hours minimum"],
    )

    try:
        minor_min_courses = int(minor_courses_cfg.get("min_courses", 5))
    except Exception:
        minor_min_courses = 5
    try:
        minor_min_hours = float(minor_hours_cfg.get("min_hours", 15))
    except Exception:
        minor_min_hours = 15.0
    try:
        minor_min_upper = int(minor_upper_cfg.get("min_count", 3))
    except Exception:
        minor_min_upper = 3
    try:
        minor_upper_level = int(minor_upper_cfg.get("min_level", upper_min))
    except Exception:
        minor_upper_level = upper_min
    try:
        double_major_min_additional = float(dm_add_cfg.get("min_additional_hours", 12))
    except Exception:
        double_major_min_additional = 12.0

    course_level = {}
    for cid, c in course_by_id.items():
        m = re.search(r"(\d{3})", str(c.course_number or ""))
        course_level[cid] = int(m.group(1)) if m else None

    for mn in selected_minors:
        minor_courses = set(program_course_sets.get(mn.id, set()))
        minor_credits = float(program_credit_sums.get(mn.id, 0.0))
        upper_count = sum(1 for cid in minor_courses if (course_level.get(cid) or 0) >= minor_upper_level)
        if minor_courses_rule:
            out.append(
                {
                    "rule_code": str(minor_courses_rule.rule_code or "").strip(),
                    "rule_name": minor_courses_rule.name,
                    "status": ("PASS" if len(minor_courses) >= minor_min_courses else "FAIL"),
                    "message": f"Minor - {mn.name}: courses {len(minor_courses)}; minimum {minor_min_courses}.",
                }
            )
        if minor_hours_rule:
            out.append(
                {
                    "rule_code": str(minor_hours_rule.rule_code or "").strip(),
                    "rule_name": minor_hours_rule.name,
                    "status": ("PASS" if minor_credits >= minor_min_hours else "FAIL"),
                    "message": f"Minor - {mn.name}: hours {minor_credits:.1f}; minimum {minor_min_hours:.1f}.",
                }
            )
        if minor_upper_rule:
            out.append(
                {
                    "rule_code": str(minor_upper_rule.rule_code or "").strip(),
                    "rule_name": minor_upper_rule.name,
                    "status": ("PASS" if upper_count >= minor_min_upper else "FAIL"),
                    "message": f"Minor - {mn.name}: {minor_upper_level}+ level courses {upper_count}; minimum {minor_min_upper}.",
                }
            )

    for a, b in itertools.combinations(selected_majors, 2):
        oriented_pairs: list[tuple[AcademicProgram, AcademicProgram]] = [(a, b), (b, a)]
        for primary, secondary in oriented_pairs:
            same_division_conflict = bool(primary.division and secondary.division and primary.division == secondary.division)

            p_courses = set(program_course_sets.get(primary.id, set()))
            s_courses = set(program_course_sets.get(secondary.id, set()))
            p_credits = float(program_credit_sums.get(primary.id, 0.0))
            union_course_ids = p_courses | s_courses
            union_credits = sum(float(course_by_id[cid].credit_hours or 0.0) for cid in union_course_ids if cid in course_by_id)
            additional_vs_primary = union_credits - p_credits

            if dm_div_rule:
                out.append(
                    {
                        "rule_code": str(dm_div_rule.rule_code or "").strip(),
                        "rule_name": dm_div_rule.name,
                        "status": ("FAIL" if same_division_conflict else "PASS"),
                        "message": (
                            f"Double Major ({a.name} + {b.name}), perspective {primary.name}: "
                            f"majors must be in separate divisions."
                        ),
                    }
                )
            if dm_add_rule:
                out.append(
                    {
                        "rule_code": str(dm_add_rule.rule_code or "").strip(),
                        "rule_name": dm_add_rule.name,
                        "status": ("PASS" if additional_vs_primary >= double_major_min_additional else "FAIL"),
                        "message": (
                            f"Double Major ({a.name} + {b.name}), perspective {primary.name}: additional hours "
                            f"{additional_vs_primary:.1f}; minimum {double_major_min_additional:.1f}."
                        ),
                    }
                )
    return out


def find_active_rule(
    rules_with_cfg: list[tuple[ValidationRule, dict]],
    *,
    rule_type: Optional[str] = None,
    names: Optional[list[str]] = None,
) -> tuple[Optional[ValidationRule], dict]:
    names_lc = {str(n).strip().lower() for n in (names or [])}
    target_type = str(rule_type or "").upper().strip()
    for rule, cfg in rules_with_cfg:
        if target_type and str(cfg.get("type") or "").upper().strip() == target_type:
            return rule, cfg
    if names_lc:
        for rule, cfg in rules_with_cfg:
            if str(rule.name or "").strip().lower() in names_lc:
                return rule, cfg
    return None, {}


def build_program_designer_code_map(reqs: list[Requirement], program_by_id: dict[str, AcademicProgram]) -> dict[str, str]:
    by_parent: dict[Optional[str], list[Requirement]] = {}
    for r in reqs:
        by_parent.setdefault(r.parent_requirement_id, []).append(r)
    for k in by_parent:
        by_parent[k] = sorted(by_parent[k], key=lambda x: (int(x.sort_order or 0), str(x.name or "").lower()))

    roots = by_parent.get(None, [])
    major_program_ids = sorted(
        {r.program_id for r in roots if r.program_id and (str(r.category or "").upper() == "MAJOR")},
        key=lambda pid: str((program_by_id.get(pid).name if program_by_id.get(pid) else pid) or "").lower(),
    )
    minor_program_ids = sorted(
        {r.program_id for r in roots if r.program_id and (str(r.category or "").upper() == "MINOR")},
        key=lambda pid: str((program_by_id.get(pid).name if program_by_id.get(pid) else pid) or "").lower(),
    )
    major_idx = {pid: i + 1 for i, pid in enumerate(major_program_ids)}
    minor_idx = {pid: i + 1 for i, pid in enumerate(minor_program_ids)}

    core_roots = [r for r in roots if (str(r.category or "").upper() == "CORE")]
    core_root_seen = 0

    code_map: dict[str, str] = {}

    def child_suffix(req: Requirement, sibling_counts: dict[str, int]) -> str:
        cat = str(req.category or "").upper()
        mm = str(req.major_mode or "").upper()
        has_track = bool(str(req.track_name or "").strip())
        key = "R"
        if cat == "CORE":
            key = "T" if (mm == "TRACK" or has_track) else "R"
        elif cat in {"MAJOR", "MINOR"}:
            if mm == "REQUIREMENT":
                key = "M"
            elif mm == "TRACK" or has_track:
                key = "T"
            else:
                key = "R"
        sibling_counts[key] = sibling_counts.get(key, 0) + 1
        n = sibling_counts[key]
        if key == "M":
            return f".M{n}"
        if key == "T":
            return f".T{n}"
        return f".R{n}"

    def walk(parent_id: Optional[str], parent_code: Optional[str]) -> None:
        children = by_parent.get(parent_id, [])
        sibling_counts: dict[str, int] = {}
        nonlocal core_root_seen
        for req in children:
            code = ""
            if parent_code is None:
                cat = str(req.category or "").upper()
                if cat == "CORE":
                    core_root_seen += 1
                    code = f"C{core_root_seen}"
                elif cat == "MAJOR":
                    code = f"M{major_idx.get(req.program_id, 1)}"
                elif cat == "MINOR":
                    code = f"N{minor_idx.get(req.program_id, 1)}"
                elif cat == "PE":
                    code = "PE1"
                else:
                    code = f"R{len(code_map) + 1}"
            else:
                code = f"{parent_code}{child_suffix(req, sibling_counts)}"
            code_map[req.id] = code
            walk(req.id, code)

    walk(None, None)
    return code_map


def seed_demo_data(db: Session, actor_user_id: Optional[str] = None) -> dict:
    created = {
        "versions": 0,
        "courses": 0,
        "programs": 0,
        "requirements": 0,
        "plan_items": 0,
        "prerequisites": 0,
        "substitutions": 0,
        "sections": 0,
        "cadets": 0,
        "cadet_records": 0,
        "comments": 0,
        "change_requests": 0,
        "cohort_assignments": 0,
        "equivalencies": 0,
    }

    active = db.scalar(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE").order_by(CurriculumVersion.created_at.desc()))
    if not active:
        baseline = db.scalar(select(CurriculumVersion).where(CurriculumVersion.name == "COI 2025-2026"))
        if baseline:
            baseline.status = "ACTIVE"
            if baseline.effective_start_year is None:
                baseline.effective_start_year = 2025
            if baseline.total_credit_hours is None:
                baseline.total_credit_hours = 125
            if baseline.core_credit_hours is None:
                baseline.core_credit_hours = 93
            active = baseline
            db.flush()
        else:
            active = CurriculumVersion(
                name="COI 2025-2026",
                status="ACTIVE",
                effective_start_year=2025,
                total_credit_hours=125,
                core_credit_hours=93,
            )
            db.add(active)
            db.flush()
            created["versions"] += 1

    draft = db.scalar(select(CurriculumVersion).where(CurriculumVersion.name == "COI 2025-2026 Draft A"))
    if not draft:
        draft = CurriculumVersion(
            name="COI 2025-2026 Draft A",
            status="DRAFT",
            parent_version_id=active.id,
            effective_start_year=2025,
            total_credit_hours=125,
            core_credit_hours=93,
        )
        db.add(draft)
        db.flush()
        created["versions"] += 1

    active_courses = db.scalars(select(Course).where(Course.version_id == active.id)).all()
    if not active_courses:
        baseline = [
            ("CS 110", "Introduction to Computing", 3.0, 1),
            ("MATH 141", "Calculus I", 3.0, 1),
            ("ENGR 100", "Foundations of Engineering", 3.0, 1),
            ("CS 210", "Data Structures", 3.0, 2),
            ("MATH 142", "Calculus II", 3.0, 2),
            ("PHYS 110", "General Physics I", 3.0, 2),
            ("CS 310", "Algorithms", 3.0, 3),
            ("CS 340", "Computer Architecture", 3.0, 3),
        ]
        for number, title, credits, semester in baseline:
            db.add(
                Course(
                    version_id=active.id,
                    course_number=number,
                    title=title,
                    credit_hours=credits,
                    designated_semester=semester,
                    min_section_size=6,
                )
            )
            created["courses"] += 1
        db.flush()
        active_courses = db.scalars(select(Course).where(Course.version_id == active.id)).all()

    active_courses_by_number = {c.course_number: c for c in active_courses}

    draft_courses = db.scalars(select(Course).where(Course.version_id == draft.id)).all()
    if not draft_courses:
        draft_seed = [
            ("CS 110", "Introduction to Computing", 3.0, 1),
            ("CS 210", "Data Structures", 3.0, 2),
            ("CS 310", "Algorithms and Complexity", 3.0, 3),
            ("MATH 141", "Calculus I", 3.0, 1),
        ]
        for number, title, credits, semester in draft_seed:
            db.add(
                Course(
                    version_id=draft.id,
                    course_number=number,
                    title=title,
                    credit_hours=credits,
                    designated_semester=semester,
                    min_section_size=6,
                )
            )
            created["courses"] += 1
        db.flush()
        draft_courses = db.scalars(select(Course).where(Course.version_id == draft.id)).all()
    draft_courses_by_number = {c.course_number: c for c in draft_courses}

    cs_program = db.scalar(
        select(AcademicProgram).where(AcademicProgram.version_id == active.id, AcademicProgram.name == "Computer Science")
    )
    if not cs_program:
        cs_program = AcademicProgram(
            version_id=active.id, name="Computer Science", program_type="MAJOR", division="ENGINEERING_SCIENCES"
        )
        db.add(cs_program)
        db.flush()
        created["programs"] += 1
    elif not cs_program.division:
        cs_program.division = "ENGINEERING_SCIENCES"

    core_req = db.scalar(
        select(Requirement).where(Requirement.version_id == active.id, Requirement.name == "Core Requirements")
    )
    if not core_req:
        core_req = Requirement(
            version_id=active.id,
            parent_requirement_id=None,
            program_id=None,
            name="Core Requirements",
            logic_type="ALL_REQUIRED",
            sort_order=0,
        )
        db.add(core_req)
        db.flush()
        created["requirements"] += 1

    cs_found_req = db.scalar(
        select(Requirement).where(Requirement.version_id == active.id, Requirement.name == "CS Foundations")
    )
    if not cs_found_req:
        cs_found_req = Requirement(
            version_id=active.id,
            parent_requirement_id=core_req.id,
            program_id=cs_program.id,
            name="CS Foundations",
            logic_type="ALL_REQUIRED",
            sort_order=1,
        )
        db.add(cs_found_req)
        db.flush()
        created["requirements"] += 1

    if not db.scalar(select(PlanItem).where(PlanItem.version_id == active.id)):
        sorted_courses = sorted(active_courses, key=lambda c: (c.designated_semester or 99, c.course_number))
        for idx, course in enumerate(sorted_courses):
            db.add(
                PlanItem(
                    version_id=active.id,
                    semester_index=(course.designated_semester or 1),
                    course_id=course.id,
                    position=idx,
                    aspect="CORE",
                )
            )
            created["plan_items"] += 1

    cs110 = active_courses_by_number.get("CS 110")
    cs210 = active_courses_by_number.get("CS 210")
    cs310 = active_courses_by_number.get("CS 310")
    math141 = active_courses_by_number.get("MATH 141")
    math142 = active_courses_by_number.get("MATH 142")

    if cs210 and cs110 and not db.scalar(
        select(CoursePrerequisite).where(CoursePrerequisite.course_id == cs210.id, CoursePrerequisite.required_course_id == cs110.id)
    ):
        db.add(
            CoursePrerequisite(
                course_id=cs210.id,
                required_course_id=cs110.id,
                relationship_type="PREREQUISITE",
                enforcement="HARD",
            )
        )
        created["prerequisites"] += 1

    if cs310 and cs210 and not db.scalar(
        select(CoursePrerequisite).where(CoursePrerequisite.course_id == cs310.id, CoursePrerequisite.required_course_id == cs210.id)
    ):
        db.add(
            CoursePrerequisite(
                course_id=cs310.id,
                required_course_id=cs210.id,
                relationship_type="PREREQUISITE",
                enforcement="HARD",
            )
        )
        created["prerequisites"] += 1

    if math142 and math141 and not db.scalar(
        select(CourseSubstitution).where(
            CourseSubstitution.original_course_id == math142.id,
            CourseSubstitution.substitute_course_id == math141.id,
        )
    ):
        db.add(
            CourseSubstitution(
                original_course_id=math142.id,
                substitute_course_id=math141.id,
                is_bidirectional=False,
                requires_approval=True,
                conditions_json=json.dumps({"minimum_grade": "C"}),
            )
        )
        created["substitutions"] += 1

    if cs_found_req and cs110 and not db.scalar(
        select(RequirementFulfillment).where(RequirementFulfillment.requirement_id == cs_found_req.id, RequirementFulfillment.course_id == cs110.id)
    ):
        db.add(RequirementFulfillment(requirement_id=cs_found_req.id, course_id=cs110.id, is_primary=True))
    if cs_found_req and cs210 and not db.scalar(
        select(RequirementFulfillment).where(RequirementFulfillment.requirement_id == cs_found_req.id, RequirementFulfillment.course_id == cs210.id)
    ):
        db.add(RequirementFulfillment(requirement_id=cs_found_req.id, course_id=cs210.id, is_primary=True))

    instructor = db.scalar(select(Instructor).where(Instructor.name == "Sample Instructor"))
    if not instructor:
        instructor = Instructor(name="Sample Instructor", department="CS", max_sections_per_semester=3)
        db.add(instructor)
        db.flush()
    if not db.scalar(select(Instructor).where(Instructor.name == "Sample Instructor B")):
        db.add(Instructor(name="Sample Instructor B", department="MATH", max_sections_per_semester=2))
        db.flush()

    room = db.scalar(select(Classroom).where(Classroom.building == "Fairchild", Classroom.room_number == "2G13"))
    if not room:
        room = Classroom(building="Fairchild", room_number="2G13", capacity=24, room_type="CLASSROOM")
        db.add(room)
        db.flush()
    if not db.scalar(select(Classroom).where(Classroom.building == "Fairchild", Classroom.room_number == "3E10")):
        db.add(Classroom(building="Fairchild", room_number="3E10", capacity=20, room_type="LAB"))
        db.flush()

    if not db.scalar(select(Section).where(Section.version_id == active.id)) and cs110 and cs210:
        db.add(
            Section(
                version_id=active.id,
                course_id=cs110.id,
                semester_label="Fall 2025",
                max_enrollment=18,
                instructor_id=instructor.id if instructor else None,
                classroom_id=room.id if room else None,
            )
        )
        db.add(
            Section(
                version_id=active.id,
                course_id=cs210.id,
                semester_label="Spring 2026",
                max_enrollment=18,
                instructor_id=instructor.id if instructor else None,
                classroom_id=room.id if room else None,
            )
        )
        created["sections"] += 2

    if not db.scalar(select(Cadet).where(Cadet.name == "Cadet Demo One")):
        db.add(Cadet(name="Cadet Demo One", class_year=2029, major_program_id=cs_program.id, cumulative_gpa=3.2))
        created["cadets"] += 1
    if not db.scalar(select(Cadet).where(Cadet.name == "Cadet Demo Two")):
        db.add(Cadet(name="Cadet Demo Two", class_year=2030, major_program_id=cs_program.id, cumulative_gpa=2.9))
        created["cadets"] += 1
    db.flush()

    cadet_one = db.scalar(select(Cadet).where(Cadet.name == "Cadet Demo One"))
    if cadet_one and cs110 and not db.scalar(
        select(CadetRecord).where(CadetRecord.cadet_id == cadet_one.id, CadetRecord.course_id == cs110.id)
    ):
        db.add(CadetRecord(cadet_id=cadet_one.id, course_id=cs110.id, semester_label="Fall 2025", grade="A-", is_completed=True))
        created["cadet_records"] += 1

    if actor_user_id and not db.scalar(select(DesignComment).where(DesignComment.version_id == active.id)):
        db.add(
            DesignComment(
                version_id=active.id,
                entity_type="CurriculumVersion",
                entity_id=active.id,
                comment="Demo comment: baseline loaded for QC.",
                created_by=actor_user_id,
            )
        )
        created["comments"] += 1

    if actor_user_id and not db.scalar(select(ChangeRequest).where(ChangeRequest.version_id == active.id)):
        db.add(
            ChangeRequest(
                version_id=active.id,
                title="Demo change request",
                description="Review CS 310 title update in draft.",
                status="PROPOSED",
                proposed_by=actor_user_id,
            )
        )
        created["change_requests"] += 1

    if not db.scalar(
        select(CohortAssignment).where(CohortAssignment.class_year == 2029, CohortAssignment.version_id == active.id)
    ):
        db.add(CohortAssignment(class_year=2029, version_id=active.id))
        created["cohort_assignments"] += 1

    if (
        cs310
        and draft_courses_by_number.get("CS 310")
        and not db.scalar(
            select(CourseEquivalency).where(
                CourseEquivalency.from_version_id == active.id,
                CourseEquivalency.to_version_id == draft.id,
                CourseEquivalency.from_course_id == cs310.id,
                CourseEquivalency.to_course_id == draft_courses_by_number["CS 310"].id,
            )
        )
    ):
        db.add(
            CourseEquivalency(
                from_version_id=active.id,
                to_version_id=draft.id,
                from_course_id=cs310.id,
                to_course_id=draft_courses_by_number["CS 310"].id,
            )
        )
        created["equivalencies"] += 1

    return created


@app.on_event("startup")
def startup():
    Base.metadata.create_all(engine)
    ensure_runtime_migrations()
    with SessionLocal() as db:
        ensure_period_config_migration(db)
        if not db.scalar(select(User).where(User.username == "design_admin")):
            db.add(User(username="design_admin", password="design_admin", role="DESIGN"))
            db.add(User(username="advisor_user", password="advisor_user", role="ADVISOR"))
        if not db.scalar(select(CurriculumVersion).where(CurriculumVersion.name == "COI 2025-2026")):
            db.add(CurriculumVersion(name="COI 2025-2026", status="ACTIVE", effective_start_year=2025, total_credit_hours=125, core_credit_hours=93))
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Minimum section size >= 6")):
            db.add(ValidationRule(name="Minimum section size >= 6", tier=1, severity="WARNING", active=True, config_json=json.dumps({"minimum": 6})))
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Semester credit upper bound")):
            db.add(ValidationRule(name="Semester credit upper bound", tier=1, severity="WARNING", active=True, config_json=json.dumps({"max_credits": 24})))
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Prerequisite ordering")):
            db.add(ValidationRule(name="Prerequisite ordering", tier=1, severity="FAIL", active=True, config_json=json.dumps({})))
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Program feasibility gate")):
            db.add(
                ValidationRule(
                    name="Program feasibility gate",
                    tier=1,
                    severity="FAIL",
                    active=True,
                    config_json=json.dumps({"type": "PROGRAM_FEASIBILITY_GATE"}),
                )
            )
        # Replace legacy bundled residency rule with editable split rules.
        legacy_residency = db.scalar(select(ValidationRule).where(ValidationRule.name == "COI residency requirements"))
        if legacy_residency:
            db.delete(legacy_residency)
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Residency minimum in-residence hours")):
            db.add(
                ValidationRule(
                    name="Residency minimum in-residence hours",
                    tier=1,
                    severity="WARNING",
                    active=True,
                    config_json=json.dumps({"type": "RESIDENCY_MIN_HOURS", "min_hours": DEFAULT_RESIDENCY_MIN_HOURS}),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Residency minimum academic semesters")):
            db.add(
                ValidationRule(
                    name="Residency minimum academic semesters",
                    tier=1,
                    severity="WARNING",
                    active=True,
                    config_json=json.dumps({"type": "RESIDENCY_MIN_ACADEMIC_SEMESTERS", "min_semesters": DEFAULT_RESIDENCY_MIN_ACADEMIC_SEMESTERS}),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Instructor load limits")):
            db.add(ValidationRule(name="Instructor load limits", tier=3, severity="WARNING", active=True, config_json=json.dumps({})))
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Classroom capacity constraints")):
            db.add(ValidationRule(name="Classroom capacity constraints", tier=3, severity="WARNING", active=True, config_json=json.dumps({})))
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Instructor qualification constraints")):
            db.add(ValidationRule(name="Instructor qualification constraints", tier=3, severity="WARNING", active=True, config_json=json.dumps({})))
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "ABET-EAC Computer Science placeholder")):
            db.add(
                ValidationRule(
                    name="ABET-EAC Computer Science placeholder",
                    tier=2,
                    severity="WARNING",
                    active=True,
                    config_json=json.dumps({"program": "Computer Science", "type": "abet_placeholder"}),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "ABET-EAC Electrical Engineering placeholder")):
            db.add(
                ValidationRule(
                    name="ABET-EAC Electrical Engineering placeholder",
                    tier=2,
                    severity="WARNING",
                    active=True,
                    config_json=json.dumps({"program": "Electrical Engineering", "type": "abet_placeholder"}),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "ABET-EAC Mechanical Engineering placeholder")):
            db.add(
                ValidationRule(
                    name="ABET-EAC Mechanical Engineering placeholder",
                    tier=2,
                    severity="WARNING",
                    active=True,
                    config_json=json.dumps({"program": "Mechanical Engineering", "type": "abet_placeholder"}),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "ABET EAC: Math/Basic Science Credits Minimum")):
            db.add(
                ValidationRule(
                    name="ABET EAC: Math/Basic Science Credits Minimum",
                    tier=2,
                    severity="FAIL",
                    active=False,
                    config_json=json.dumps(
                        {
                            "domain": "Accreditation",
                            "type": "ABET_EAC_MATH_BASIC_SCI_MIN",
                            "bucket_code": "ABET_MATH_BASIC_SCI",
                            "min_credits": 30,
                        }
                    ),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "ABET EAC: Engineering Topics Credits Minimum")):
            db.add(
                ValidationRule(
                    name="ABET EAC: Engineering Topics Credits Minimum",
                    tier=2,
                    severity="FAIL",
                    active=False,
                    config_json=json.dumps(
                        {
                            "domain": "Accreditation",
                            "type": "ABET_EAC_ENGINEERING_TOPICS_MIN",
                            "bucket_code": "ABET_ENGINEERING_TOPICS",
                            "min_credits": 45,
                        }
                    ),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Cadet Performance: Minimum cumulative GPA")):
            db.add(
                ValidationRule(
                    name="Cadet Performance: Minimum cumulative GPA",
                    tier=1,
                    severity="FAIL",
                    active=False,
                    config_json=json.dumps({"domain": "Cadet Performance", "type": "MIN_CUM_GPA", "min_gpa": 2.0}),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Cadet Performance: Minimum core GPA")):
            db.add(
                ValidationRule(
                    name="Cadet Performance: Minimum core GPA",
                    tier=1,
                    severity="FAIL",
                    active=False,
                    config_json=json.dumps({"domain": "Cadet Performance", "type": "MIN_CORE_GPA", "min_gpa": 2.0}),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Program/Major Pathway: Minor minimum courses")):
            db.add(
                ValidationRule(
                    name="Program/Major Pathway: Minor minimum courses",
                    tier=2,
                    severity="FAIL",
                    active=True,
                    config_json=json.dumps({"domain": "Program/Major Pathway", "type": "MINOR_MIN_COURSES", "min_courses": 5}),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Program/Major Pathway: Minor minimum hours")):
            db.add(
                ValidationRule(
                    name="Program/Major Pathway: Minor minimum hours",
                    tier=2,
                    severity="FAIL",
                    active=True,
                    config_json=json.dumps({"domain": "Program/Major Pathway", "type": "MINOR_MIN_HOURS", "min_hours": 15}),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Program/Major Pathway: Minor upper-level courses minimum")):
            db.add(
                ValidationRule(
                    name="Program/Major Pathway: Minor upper-level courses minimum",
                    tier=2,
                    severity="FAIL",
                    active=True,
                    config_json=json.dumps(
                        {"domain": "Program/Major Pathway", "type": "MINOR_MIN_UPPER_LEVEL_COURSES", "min_count": 3, "min_level": 300}
                    ),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Program/Major Pathway: Double major divisional separation")):
            db.add(
                ValidationRule(
                    name="Program/Major Pathway: Double major divisional separation",
                    tier=2,
                    severity="FAIL",
                    active=True,
                    config_json=json.dumps({"domain": "Program/Major Pathway", "type": "DOUBLE_MAJOR_DIVISION_SEPARATION"}),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Program/Major Pathway: Double major additional hours minimum")):
            db.add(
                ValidationRule(
                    name="Program/Major Pathway: Double major additional hours minimum",
                    tier=2,
                    severity="FAIL",
                    active=True,
                    config_json=json.dumps(
                        {"domain": "Program/Major Pathway", "type": "DOUBLE_MAJOR_ADDITIONAL_HOURS_MIN", "min_additional_hours": 12}
                    ),
                )
            )
        if not db.scalar(select(ValidationRule).where(ValidationRule.name == "Program/Major Pathway Definition: Upper-level course number minimum")):
            db.add(
                ValidationRule(
                    name="Program/Major Pathway Definition: Upper-level course number minimum",
                    tier=2,
                    severity="WARNING",
                    active=True,
                    config_json=json.dumps(
                        {"domain": "Definitional", "type": "DEF_UPPER_LEVEL_COURSE_NUMBER", "min_level": 300}
                    ),
                )
            )
        else:
            def_upper = db.scalar(select(ValidationRule).where(ValidationRule.name == "Program/Major Pathway Definition: Upper-level course number minimum"))
            if def_upper:
                try:
                    cfg = json.loads(def_upper.config_json or "{}")
                except Exception:
                    cfg = {}
                if not isinstance(cfg, dict):
                    cfg = {}
                cfg["domain"] = "Definitional"
                cfg.setdefault("type", "DEF_UPPER_LEVEL_COURSE_NUMBER")
                cfg.setdefault("min_level", 300)
                def_upper.config_json = json.dumps(cfg)

        old_def_div = db.scalar(select(ValidationRule).where(ValidationRule.name == "Program/Major Pathway Definition: Divisional major names"))
        if old_def_div:
            db.delete(old_def_div)
        old_def_basis = db.scalar(select(ValidationRule).where(ValidationRule.name == "Program/Major Pathway Definition: Double major additional-hours basis"))
        if old_def_basis:
            db.delete(old_def_basis)
        ensure_validation_rule_codes(db)
        for prog in db.scalars(select(AcademicProgram)).all():
            if prog.program_type == "MAJOR" and not prog.division:
                prog.division = infer_division_from_program_name(prog.name)
        db.commit()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/demo/load-data")
def load_demo_data(db: Session = Depends(get_db), user: User = Depends(require_design)):
    summary = seed_demo_data(db, actor_user_id=user.id)
    db.commit()
    write_audit(db, user, "SEED_DEMO_DATA", "System", "demo", json.dumps(summary))
    return {"status": "ok", "summary": summary}


@app.post("/auth/login")
def login(payload: LoginIn, db: Session = Depends(get_db)):
    user = db.scalar(select(User).where(User.username == payload.username))
    if not user or user.password != payload.password:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"session_token": serializer.dumps({"user_id": user.id}), "role": user.role}


@app.post("/versions", response_model=VersionOut)
def create_version(payload: VersionIn, db: Session = Depends(get_db), user: User = Depends(require_design)):
    v = CurriculumVersion(**payload.model_dump(), status="DRAFT")
    db.add(v)
    db.commit()
    db.refresh(v)
    write_audit(db, user, "CREATE", "CurriculumVersion", v.id, str(payload.model_dump()))
    return serialize(v)


@app.get("/versions", response_model=list[VersionOut])
def list_versions(db: Session = Depends(get_db), _: User = Depends(current_user)):
    return [serialize(v) for v in db.scalars(select(CurriculumVersion).order_by(CurriculumVersion.created_at.desc())).all()]


@app.post("/versions/{version_id}/branch", response_model=VersionOut)
def branch(version_id: str, name: str, db: Session = Depends(get_db), user: User = Depends(require_design)):
    src = db.get(CurriculumVersion, version_id)
    if not src:
        raise HTTPException(status_code=404, detail="Version not found")
    v = CurriculumVersion(name=name, parent_version_id=src.id, status="DRAFT", effective_start_year=src.effective_start_year)
    db.add(v)
    db.commit()
    db.refresh(v)
    write_audit(db, user, "BRANCH", "CurriculumVersion", v.id, f"parent={src.id}")
    return serialize(v)


@app.post("/versions/{version_id}/activate")
def activate(version_id: str, db: Session = Depends(get_db), user: User = Depends(require_design)):
    target = db.get(CurriculumVersion, version_id)
    if not target:
        raise HTTPException(status_code=404, detail="Version not found")
    for v in db.scalars(select(CurriculumVersion).where(CurriculumVersion.status == "ACTIVE")).all():
        v.status = "ARCHIVED"
    target.status = "ACTIVE"
    db.commit()
    write_audit(db, user, "ACTIVATE", "CurriculumVersion", target.id)
    return {"status": "activated"}


@app.get("/versions/compare")
def compare(from_id: str, to_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    a = {c.course_number: c for c in db.scalars(select(Course).where(Course.version_id == from_id)).all()}
    b = {c.course_number: c for c in db.scalars(select(Course).where(Course.version_id == to_id)).all()}
    return {"added": sorted(list(set(b) - set(a))), "removed": sorted(list(set(a) - set(b))), "common": len(set(a) & set(b))}


@app.post("/versions/{version_id}/status")
def set_version_status(version_id: str, status: str, db: Session = Depends(get_db), user: User = Depends(require_design)):
    allowed = {"DRAFT", "UNDER_REVIEW", "APPROVED", "ACTIVE", "ARCHIVED"}
    if status not in allowed:
        raise HTTPException(status_code=400, detail="Invalid status")
    v = db.get(CurriculumVersion, version_id)
    if not v:
        raise HTTPException(status_code=404, detail="Version not found")
    v.status = status
    db.commit()
    write_audit(db, user, "STATUS", "CurriculumVersion", v.id, status)
    return {"status": v.status}


@app.get("/design/versioning/diff/{from_id}/{to_id}")
def detailed_diff(from_id: str, to_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    from_courses = {c.course_number: c for c in db.scalars(select(Course).where(Course.version_id == from_id)).all()}
    to_courses = {c.course_number: c for c in db.scalars(select(Course).where(Course.version_id == to_id)).all()}

    changed = []
    for num in sorted(set(from_courses) & set(to_courses)):
        left = from_courses[num]
        right = to_courses[num]
        if left.title != right.title or left.credit_hours != right.credit_hours or left.designated_semester != right.designated_semester:
            changed.append(
                {
                    "course_number": num,
                    "from": {"title": left.title, "credit_hours": left.credit_hours, "designated_semester": left.designated_semester},
                    "to": {"title": right.title, "credit_hours": right.credit_hours, "designated_semester": right.designated_semester},
                }
            )

    from_reqs = {r.name for r in db.scalars(select(Requirement).where(Requirement.version_id == from_id)).all()}
    to_reqs = {r.name for r in db.scalars(select(Requirement).where(Requirement.version_id == to_id)).all()}

    return {
        "courses_added": sorted(list(set(to_courses) - set(from_courses))),
        "courses_removed": sorted(list(set(from_courses) - set(to_courses))),
        "courses_changed": changed,
        "requirements_added": sorted(list(to_reqs - from_reqs)),
        "requirements_removed": sorted(list(from_reqs - to_reqs)),
    }


@app.post("/courses")
def create_course(payload: CourseIn, db: Session = Depends(get_db), user: User = Depends(require_design)):
    c = Course(**payload.model_dump())
    db.add(c)
    db.commit()
    db.refresh(c)
    write_audit(db, user, "CREATE", "Course", c.id, str(payload.model_dump()))
    return serialize(c)


@app.put("/courses/{course_id}")
def update_course(course_id: str, payload: CourseIn, db: Session = Depends(get_db), user: User = Depends(require_design)):
    c = db.get(Course, course_id)
    if not c:
        raise HTTPException(status_code=404, detail="Course not found")
    for k, v in payload.model_dump().items():
        setattr(c, k, v)
    db.commit()
    db.refresh(c)
    write_audit(db, user, "UPDATE", "Course", c.id, str(payload.model_dump()))
    return serialize(c)


@app.delete("/courses/{course_id}")
def delete_course(course_id: str, db: Session = Depends(get_db), user: User = Depends(require_design)):
    c = db.get(Course, course_id)
    if not c:
        raise HTTPException(status_code=404, detail="Course not found")
    db.delete(c)
    db.commit()
    write_audit(db, user, "DELETE", "Course", course_id)
    return {"status": "deleted"}


@app.get("/courses")
def list_courses(
    version_id: Optional[str] = None,
    q: Optional[str] = None,
    sort_by: str = "course_number",
    order: str = "asc",
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _: User = Depends(current_user),
):
    stmt = select(Course)
    if version_id:
        stmt = stmt.where(Course.version_id == version_id)
    if q:
        stmt = stmt.where((Course.course_number.contains(q)) | (Course.title.contains(q)))
    col = getattr(Course, sort_by, Course.course_number)
    stmt = stmt.order_by(col.desc() if order == "desc" else col.asc()).limit(limit).offset(offset)
    return [serialize(c) for c in db.scalars(stmt).all()]


@app.get("/courses/{course_id}")
def get_course(course_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    course = db.get(Course, course_id)
    if not course:
        raise HTTPException(status_code=404, detail="Course not found")
    prereqs = db.scalars(select(CoursePrerequisite).where(CoursePrerequisite.course_id == course_id)).all()
    return {"course": serialize(course), "prerequisites": [serialize(p) for p in prereqs]}


@app.get("/courses/{course_id}/buckets")
def list_course_buckets(course_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    course = db.get(Course, course_id)
    if not course:
        raise HTTPException(status_code=404, detail="Course not found")
    rows = db.scalars(
        select(CourseBucketTag)
        .where(CourseBucketTag.course_id == course_id)
        .order_by(CourseBucketTag.sort_order.asc(), CourseBucketTag.bucket_code.asc())
    ).all()
    return [serialize(r) for r in rows]


@app.get("/courses/buckets/version/{version_id}")
def list_version_course_buckets(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    course_by_id = {c.id: c for c in courses}
    if not course_by_id:
        return []
    rows = db.scalars(select(CourseBucketTag).where(CourseBucketTag.course_id.in_(list(course_by_id.keys())))).all()
    return [
        {
            **serialize(r),
            "course_number": (course_by_id.get(r.course_id).course_number if course_by_id.get(r.course_id) else None),
            "course_title": (course_by_id.get(r.course_id).title if course_by_id.get(r.course_id) else None),
        }
        for r in rows
    ]


@app.post("/courses/{course_id}/buckets")
def upsert_course_bucket(
    course_id: str, payload: CourseBucketTagIn, db: Session = Depends(get_db), user: User = Depends(require_design)
):
    course = db.get(Course, course_id)
    if not course:
        raise HTTPException(status_code=404, detail="Course not found")
    code = str(payload.bucket_code or "").strip().upper()
    if not code:
        raise HTTPException(status_code=400, detail="bucket_code is required")
    existing = db.scalar(
        select(CourseBucketTag).where(CourseBucketTag.course_id == course_id, CourseBucketTag.bucket_code == code)
    )
    if existing:
        existing.credit_hours_override = payload.credit_hours_override
        existing.sort_order = payload.sort_order
        db.commit()
        db.refresh(existing)
        write_audit(
            db, user, "UPDATE", "CourseBucketTag", existing.id, json.dumps({"course_id": course_id, "bucket_code": code})
        )
        return serialize(existing)
    row = CourseBucketTag(
        course_id=course_id,
        bucket_code=code,
        credit_hours_override=payload.credit_hours_override,
        sort_order=payload.sort_order,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    write_audit(db, user, "CREATE", "CourseBucketTag", row.id, json.dumps(payload.model_dump()))
    return serialize(row)


@app.delete("/courses/buckets/{bucket_tag_id}")
def delete_course_bucket(bucket_tag_id: str, db: Session = Depends(get_db), user: User = Depends(require_design)):
    row = db.get(CourseBucketTag, bucket_tag_id)
    if not row:
        raise HTTPException(status_code=404, detail="Course bucket tag not found")
    db.delete(row)
    db.commit()
    write_audit(db, user, "DELETE", "CourseBucketTag", bucket_tag_id)
    return {"status": "deleted"}


@app.post("/prerequisites")
def create_prerequisite(payload: PrerequisiteIn, db: Session = Depends(get_db), user: User = Depends(require_design)):
    if payload.course_id == payload.required_course_id:
        raise HTTPException(status_code=400, detail="Course cannot require itself")
    p = CoursePrerequisite(**payload.model_dump())
    db.add(p)
    db.commit()
    db.refresh(p)
    write_audit(db, user, "CREATE", "CoursePrerequisite", p.id, str(payload.model_dump()))
    return serialize(p)


@app.delete("/prerequisites/{prerequisite_id}")
def delete_prerequisite(prerequisite_id: str, db: Session = Depends(get_db), user: User = Depends(require_design)):
    p = db.get(CoursePrerequisite, prerequisite_id)
    if not p:
        raise HTTPException(status_code=404, detail="Prerequisite not found")
    db.delete(p)
    db.commit()
    write_audit(db, user, "DELETE", "CoursePrerequisite", prerequisite_id)
    return {"status": "deleted"}


@app.post("/substitutions")
def create_substitution(payload: SubstitutionIn, db: Session = Depends(get_db), user: User = Depends(require_design)):
    if payload.original_course_id == payload.substitute_course_id:
        raise HTTPException(status_code=400, detail="Course cannot substitute itself")
    obj = CourseSubstitution(
        original_course_id=payload.original_course_id,
        substitute_course_id=payload.substitute_course_id,
        is_bidirectional=payload.is_bidirectional,
        requires_approval=payload.requires_approval,
        conditions_json=json.dumps(payload.conditions or {}),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    write_audit(db, user, "CREATE", "CourseSubstitution", obj.id, str(payload.model_dump()))
    return serialize(obj)


@app.get("/substitutions/{course_id}")
def list_substitutions(course_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    rows = db.scalars(select(CourseSubstitution).where(CourseSubstitution.original_course_id == course_id)).all()
    out = []
    for row in rows:
        item = serialize(row)
        try:
            item["conditions"] = json.loads(item["conditions_json"]) if item.get("conditions_json") else {}
        except Exception:
            item["conditions"] = {}
        out.append(item)
    return out


@app.get("/prerequisites/{course_id}")
def list_prerequisites(course_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    return [serialize(p) for p in db.scalars(select(CoursePrerequisite).where(CoursePrerequisite.course_id == course_id)).all()]


@app.get("/design/prerequisite-graph/{version_id}")
def prerequisite_graph(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    ids = {c.id for c in courses}
    nodes = [{"id": c.id, "label": c.course_number, "title": c.title, "semester": c.designated_semester} for c in courses]
    edges = []
    for pre in db.scalars(select(CoursePrerequisite)).all():
        if pre.course_id in ids and pre.required_course_id in ids:
            edges.append(
                {
                    "id": pre.id,
                    "from": pre.required_course_id,
                    "to": pre.course_id,
                    "relationship_type": pre.relationship_type,
                    "enforcement": pre.enforcement,
                }
            )
    return {"nodes": nodes, "edges": edges}


@app.get("/queries/courses/by-semester/{version_id}/{semester_index}")
def query_courses_by_semester(version_id: str, semester_index: int, db: Session = Depends(get_db), _: User = Depends(current_user)):
    items = db.scalars(
        select(PlanItem).where(PlanItem.version_id == version_id, PlanItem.semester_index == semester_index).order_by(PlanItem.position.asc())
    ).all()
    out = []
    for item in items:
        course = db.get(Course, item.course_id)
        if course:
            out.append(serialize(course))
    return out


@app.get("/queries/requirements/by-program/{program_id}")
def query_requirements_by_program(program_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    return [serialize(r) for r in db.scalars(select(Requirement).where(Requirement.program_id == program_id).order_by(Requirement.sort_order.asc())).all()]


@app.get("/queries/prerequisites/{course_id}")
def query_prerequisites(course_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    return [serialize(p) for p in db.scalars(select(CoursePrerequisite).where(CoursePrerequisite.course_id == course_id)).all()]


@app.get("/queries/qualified-instructors/{course_id}")
def query_qualified_instructors(course_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    pairs = db.scalars(select(InstructorQualification).where(InstructorQualification.course_id == course_id)).all()
    out = []
    for pair in pairs:
        i = db.get(Instructor, pair.instructor_id)
        if i:
            out.append(serialize(i))
    return out


@app.post("/programs")
def create_program(payload: ProgramIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    data = payload.model_dump()
    data["division"] = normalize_division(data.get("division"))
    if not data["division"] and str(data.get("program_type") or "").upper() == "MAJOR":
        data["division"] = infer_division_from_program_name(data.get("name") or "")
    p = AcademicProgram(**data)
    db.add(p)
    db.commit()
    db.refresh(p)
    return serialize(p)


@app.put("/programs/{program_id}")
def update_program(program_id: str, payload: ProgramIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    p = db.get(AcademicProgram, program_id)
    if not p:
        raise HTTPException(status_code=404, detail="Program not found")
    data = payload.model_dump()
    data["division"] = normalize_division(data.get("division"))
    if not data["division"] and str(data.get("program_type") or p.program_type or "").upper() == "MAJOR":
        data["division"] = infer_division_from_program_name(data.get("name") or p.name)
    for k, v in data.items():
        setattr(p, k, v)
    db.commit()
    db.refresh(p)
    return serialize(p)


@app.delete("/programs/{program_id}")
def delete_program(program_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    p = db.get(AcademicProgram, program_id)
    if not p:
        raise HTTPException(status_code=404, detail="Program not found")
    db.delete(p)
    db.commit()
    return {"status": "deleted"}


@app.get("/programs")
def list_programs(version_id: Optional[str] = None, db: Session = Depends(get_db), _: User = Depends(current_user)):
    stmt = select(AcademicProgram)
    if version_id:
        stmt = stmt.where(AcademicProgram.version_id == version_id)
    return [serialize(p) for p in db.scalars(stmt.order_by(AcademicProgram.name.asc())).all()]


@app.post("/requirements")
def create_requirement(payload: RequirementIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    data = payload.model_dump()
    cat = (data.get("category") or "CORE").upper()
    if cat not in {"CORE", "MAJOR", "PE", "MINOR"}:
        raise HTTPException(status_code=400, detail="category must be one of CORE, MAJOR, MINOR, PE")
    data["category"] = cat
    logic = str(data.get("logic_type") or "ALL_REQUIRED").upper()
    if logic not in {"ALL_REQUIRED", "PICK_N", "ANY_ONE", "ONE_OF", "ANY_N", "OPTION_SLOT"}:
        raise HTTPException(status_code=400, detail="unsupported logic_type")
    data["logic_type"] = logic
    if logic in {"PICK_N", "ANY_N"}:
        data["pick_n"] = int(data.get("pick_n") or 1)
    else:
        data["pick_n"] = None
    if logic == "OPTION_SLOT":
        key = (data.get("option_slot_key") or "").strip()
        data["option_slot_key"] = key[:120] if key else None
        data["option_slot_capacity"] = int(data.get("option_slot_capacity") or 1)
    else:
        data["option_slot_key"] = None
        data["option_slot_capacity"] = None
    if cat not in {"MAJOR", "MINOR"}:
        data["major_mode"] = None
        if cat in {"CORE", "PE", "MINOR"}:
            t = (data.get("track_name") or "").strip()
            data["track_name"] = t[:120] if t else None
        else:
            data["track_name"] = None
    else:
        mode = (data.get("major_mode") or "REQUIREMENT").upper()
        if mode not in {"REQUIREMENT", "TRACK"}:
            raise HTTPException(status_code=400, detail="mode must be REQUIREMENT or TRACK")
        data["major_mode"] = mode
        if mode == "TRACK" and not (data.get("track_name") or "").strip():
            raise HTTPException(status_code=400, detail="track_name required when mode=TRACK")
        if mode != "TRACK":
            data["track_name"] = None
        elif data.get("track_name"):
            data["track_name"] = str(data["track_name"]).strip()[:120]
    if data.get("sort_order") is None:
        # Default behavior: keep siblings alphabetical by using sort_order=0.
        # Once a user manually reorders (non-zero sort_order exists), append new nodes
        # so manual cursor order remains authoritative.
        siblings = db.scalars(
            select(Requirement.sort_order).where(
                Requirement.version_id == payload.version_id, Requirement.parent_requirement_id == payload.parent_requirement_id
            )
        ).all()
        has_manual_order = any((s or 0) != 0 for s in siblings)
        data["sort_order"] = (max(siblings) + 1) if (siblings and has_manual_order) else 0
    r = Requirement(**data)
    db.add(r)
    db.commit()
    db.refresh(r)
    return serialize(r)


@app.put("/requirements/{requirement_id}")
def update_requirement(requirement_id: str, payload: RequirementIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    r = db.get(Requirement, requirement_id)
    if not r:
        raise HTTPException(status_code=404, detail="Requirement not found")
    data = payload.model_dump()
    cat = (data.get("category") or r.category or "CORE").upper()
    if cat not in {"CORE", "MAJOR", "PE", "MINOR"}:
        raise HTTPException(status_code=400, detail="category must be one of CORE, MAJOR, MINOR, PE")
    data["category"] = cat
    logic = str(data.get("logic_type") or r.logic_type or "ALL_REQUIRED").upper()
    if logic not in {"ALL_REQUIRED", "PICK_N", "ANY_ONE", "ONE_OF", "ANY_N", "OPTION_SLOT"}:
        raise HTTPException(status_code=400, detail="unsupported logic_type")
    data["logic_type"] = logic
    if logic in {"PICK_N", "ANY_N"}:
        data["pick_n"] = int(data.get("pick_n") or r.pick_n or 1)
    else:
        data["pick_n"] = None
    if logic == "OPTION_SLOT":
        key = (data.get("option_slot_key") or r.option_slot_key or "").strip()
        data["option_slot_key"] = key[:120] if key else None
        data["option_slot_capacity"] = int(data.get("option_slot_capacity") or r.option_slot_capacity or 1)
    else:
        data["option_slot_key"] = None
        data["option_slot_capacity"] = None
    if cat not in {"MAJOR", "MINOR"}:
        data["major_mode"] = None
        if cat in {"CORE", "PE", "MINOR"}:
            t = (data.get("track_name") or r.track_name or "").strip()
            data["track_name"] = t[:120] if t else None
        else:
            data["track_name"] = None
    else:
        mode = (data.get("major_mode") or r.major_mode or "REQUIREMENT").upper()
        if mode not in {"REQUIREMENT", "TRACK"}:
            raise HTTPException(status_code=400, detail="mode must be REQUIREMENT or TRACK")
        data["major_mode"] = mode
        if mode == "TRACK":
            t = (data.get("track_name") or r.track_name or "").strip()
            if not t:
                raise HTTPException(status_code=400, detail="track_name required when mode=TRACK")
            data["track_name"] = t[:120]
        else:
            data["track_name"] = None
    for k, v in data.items():
        if v is not None:
            setattr(r, k, v)
    db.commit()
    db.refresh(r)
    return serialize(r)


@app.delete("/requirements/{requirement_id}")
def delete_requirement(requirement_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    r = db.get(Requirement, requirement_id)
    if not r:
        raise HTTPException(status_code=404, detail="Requirement not found")
    db.delete(r)
    db.commit()
    return {"status": "deleted"}


@app.get("/requirements")
def list_requirements(
    version_id: Optional[str] = None,
    program_id: Optional[str] = None,
    db: Session = Depends(get_db),
    _: User = Depends(current_user),
):
    stmt = select(Requirement)
    if version_id:
        stmt = stmt.where(Requirement.version_id == version_id)
    if program_id:
        stmt = stmt.where(Requirement.program_id == program_id)
    return [serialize(r) for r in db.scalars(stmt.order_by(Requirement.sort_order.asc(), Requirement.name.asc())).all()]


@app.post("/baskets")
def create_basket(payload: CourseBasketIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    data = payload.model_dump()
    if data.get("sort_order") is None:
        siblings = db.scalars(select(CourseBasket.sort_order).where(CourseBasket.version_id == payload.version_id)).all()
        data["sort_order"] = (max(siblings) + 1) if siblings else 0
    b = CourseBasket(**data)
    db.add(b)
    db.commit()
    db.refresh(b)
    return serialize(b)


@app.put("/baskets/{basket_id}")
def update_basket(basket_id: str, payload: CourseBasketIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    row = db.get(CourseBasket, basket_id)
    if not row:
        raise HTTPException(status_code=404, detail="Basket not found")
    data = payload.model_dump()
    for k, v in data.items():
        if v is not None:
            setattr(row, k, v)
    db.commit()
    db.refresh(row)
    return serialize(row)


@app.delete("/baskets/{basket_id}")
def delete_basket(basket_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    row = db.get(CourseBasket, basket_id)
    if not row:
        raise HTTPException(status_code=404, detail="Basket not found")
    for item in db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id == basket_id)).all():
        db.delete(item)
    for link in db.scalars(select(RequirementBasketLink).where(RequirementBasketLink.basket_id == basket_id)).all():
        db.delete(link)
    db.delete(row)
    db.commit()
    return {"status": "deleted"}


@app.get("/baskets")
def list_baskets(version_id: Optional[str] = None, db: Session = Depends(get_db), _: User = Depends(current_user)):
    stmt = select(CourseBasket)
    if version_id:
        stmt = stmt.where(CourseBasket.version_id == version_id)
    rows = db.scalars(stmt.order_by(CourseBasket.sort_order.asc(), CourseBasket.name.asc())).all()
    basket_ids = [r.id for r in rows]
    items = (
        db.scalars(
            select(CourseBasketItem)
            .where(CourseBasketItem.basket_id.in_(basket_ids))
            .order_by(CourseBasketItem.basket_id.asc(), CourseBasketItem.sort_order.asc())
        ).all()
        if basket_ids
        else []
    )
    items_by_basket: dict[str, list[dict]] = {}
    for item in items:
        out = serialize(item)
        course = db.get(Course, item.course_id)
        out["course_number"] = course.course_number if course else None
        out["course_title"] = course.title if course else None
        items_by_basket.setdefault(item.basket_id, []).append(out)
    out_rows = []
    for row in rows:
        item = serialize(row)
        item["items"] = items_by_basket.get(row.id, [])
        out_rows.append(item)
    return out_rows


@app.post("/baskets/items")
def create_basket_item(payload: CourseBasketItemIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    existing = db.scalar(
        select(CourseBasketItem).where(
            CourseBasketItem.basket_id == payload.basket_id,
            CourseBasketItem.course_id == payload.course_id,
        )
    )
    if existing:
        return serialize(existing)
    data = payload.model_dump()
    if data.get("sort_order") is None:
        siblings = db.scalars(select(CourseBasketItem.sort_order).where(CourseBasketItem.basket_id == payload.basket_id)).all()
        data["sort_order"] = (max(siblings) + 1) if siblings else 0
    row = CourseBasketItem(**data)
    db.add(row)
    db.commit()
    db.refresh(row)
    return serialize(row)


@app.delete("/baskets/items/{item_id}")
def delete_basket_item(item_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    row = db.get(CourseBasketItem, item_id)
    if not row:
        raise HTTPException(status_code=404, detail="Basket item not found")
    db.delete(row)
    db.commit()
    return {"status": "deleted"}


@app.post("/baskets/items/reorder")
def reorder_basket_items(payload: list[CourseBasketItemOrderIn], db: Session = Depends(get_db), _: User = Depends(require_design)):
    updated = 0
    for item in payload:
        row = db.get(CourseBasketItem, item.item_id)
        if row:
            if item.basket_id is not None:
                row.basket_id = item.basket_id
            row.sort_order = item.sort_order
            updated += 1
    db.commit()
    return {"status": "ok", "updated": updated}


@app.post("/requirements/baskets")
def create_requirement_basket_link(payload: RequirementBasketLinkIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    if payload.max_count is not None and payload.max_count < payload.min_count:
        raise HTTPException(status_code=400, detail="max_count must be >= min_count")
    basket_item_count = len(db.scalars(select(CourseBasketItem.id).where(CourseBasketItem.basket_id == payload.basket_id)).all())
    if basket_item_count <= 0:
        raise HTTPException(status_code=400, detail="basket must contain at least one course")
    if int(payload.min_count) > basket_item_count:
        raise HTTPException(status_code=400, detail="min_count cannot exceed basket course count")
    existing = db.scalar(
        select(RequirementBasketLink).where(
            RequirementBasketLink.requirement_id == payload.requirement_id,
            RequirementBasketLink.basket_id == payload.basket_id,
        )
    )
    if existing:
        existing.min_count = payload.min_count
        existing.max_count = payload.max_count
        db.commit()
        db.refresh(existing)
        return serialize(existing)
    data = payload.model_dump()
    if data.get("sort_order") is None:
        siblings = db.scalars(
            select(RequirementBasketLink.sort_order).where(RequirementBasketLink.requirement_id == payload.requirement_id)
        ).all()
        data["sort_order"] = (max(siblings) + 1) if siblings else 0
    row = RequirementBasketLink(**data)
    db.add(row)
    db.commit()
    db.refresh(row)
    return serialize(row)


@app.put("/requirements/baskets/{link_id}")
def update_requirement_basket_link(
    link_id: str, payload: RequirementBasketLinkIn, db: Session = Depends(get_db), _: User = Depends(require_design)
):
    if payload.max_count is not None and payload.max_count < payload.min_count:
        raise HTTPException(status_code=400, detail="max_count must be >= min_count")
    basket_item_count = len(db.scalars(select(CourseBasketItem.id).where(CourseBasketItem.basket_id == payload.basket_id)).all())
    if basket_item_count <= 0:
        raise HTTPException(status_code=400, detail="basket must contain at least one course")
    if int(payload.min_count) > basket_item_count:
        raise HTTPException(status_code=400, detail="min_count cannot exceed basket course count")
    row = db.get(RequirementBasketLink, link_id)
    if not row:
        raise HTTPException(status_code=404, detail="Requirement basket link not found")
    data = payload.model_dump()
    for k, v in data.items():
        if v is not None:
            setattr(row, k, v)
    db.commit()
    db.refresh(row)
    return serialize(row)


@app.delete("/requirements/baskets/{link_id}")
def delete_requirement_basket_link(link_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    row = db.get(RequirementBasketLink, link_id)
    if not row:
        raise HTTPException(status_code=404, detail="Requirement basket link not found")
    db.delete(row)
    db.commit()
    return {"status": "deleted"}


@app.post("/requirements/baskets/reorder")
def reorder_requirement_basket_links(
    payload: list[RequirementBasketOrderIn], db: Session = Depends(get_db), _: User = Depends(require_design)
):
    updated = 0
    for item in payload:
        row = db.get(RequirementBasketLink, item.link_id)
        if row:
            if item.requirement_id is not None:
                row.requirement_id = item.requirement_id
            row.sort_order = item.sort_order
            updated += 1
    db.commit()
    return {"status": "ok", "updated": updated}


@app.get("/requirements/baskets/by-requirement/{requirement_id}")
def list_requirement_basket_links(requirement_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    rows = db.scalars(
        select(RequirementBasketLink).where(RequirementBasketLink.requirement_id == requirement_id).order_by(RequirementBasketLink.sort_order.asc())
    ).all()
    out = []
    for row in rows:
        item = serialize(row)
        basket = db.get(CourseBasket, row.basket_id)
        item["basket_name"] = basket.name if basket else None
        out.append(item)
    return out


@app.get("/requirements/baskets/version/{version_id}")
def list_requirement_basket_links_version(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    req_ids = db.scalars(select(Requirement.id).where(Requirement.version_id == version_id)).all()
    if not req_ids:
        return []
    rows = db.scalars(
        select(RequirementBasketLink)
        .where(RequirementBasketLink.requirement_id.in_(req_ids))
        .order_by(RequirementBasketLink.requirement_id.asc(), RequirementBasketLink.sort_order.asc())
    ).all()
    basket_ids = [r.basket_id for r in rows]
    baskets = {b.id: b for b in db.scalars(select(CourseBasket).where(CourseBasket.id.in_(basket_ids))).all()} if basket_ids else {}
    out = []
    for row in rows:
        item = serialize(row)
        basket = baskets.get(row.basket_id)
        item["basket_name"] = basket.name if basket else None
        out.append(item)
    return out


@app.post("/requirements/reorder")
def reorder_requirements(payload: list[RequirementOrderIn], db: Session = Depends(get_db), _: User = Depends(require_design)):
    updated = 0
    for item in payload:
        req = db.get(Requirement, item.requirement_id)
        if req:
            req.sort_order = item.sort_order
            updated += 1
    db.commit()
    return {"status": "ok", "updated": updated}


@app.post("/requirements/restructure")
def restructure_requirements(payload: list[RequirementTreeNodeIn], db: Session = Depends(get_db), _: User = Depends(require_design)):
    updated = 0
    for node in payload:
        req = db.get(Requirement, node.requirement_id)
        if req:
            req.parent_requirement_id = node.parent_requirement_id
            req.sort_order = node.sort_order
            updated += 1
    db.commit()
    return {"status": "ok", "updated": updated}


@app.get("/design/requirements/tree/{version_id}")
def requirements_tree(version_id: str, program_id: Optional[str] = None, db: Session = Depends(get_db), _: User = Depends(current_user)):
    stmt = select(Requirement).where(Requirement.version_id == version_id)
    if program_id:
        stmt = stmt.where(Requirement.program_id == program_id)
    all_reqs = db.scalars(stmt.order_by(Requirement.sort_order.asc(), Requirement.name.asc())).all()
    req_ids = [r.id for r in all_reqs]
    links = (
        db.scalars(
            select(RequirementFulfillment)
            .where(RequirementFulfillment.requirement_id.in_(req_ids))
            .order_by(RequirementFulfillment.requirement_id.asc(), RequirementFulfillment.sort_order.asc())
        ).all()
        if req_ids
        else []
    )
    basket_links = (
        db.scalars(
            select(RequirementBasketLink)
            .where(RequirementBasketLink.requirement_id.in_(req_ids))
            .order_by(RequirementBasketLink.requirement_id.asc(), RequirementBasketLink.sort_order.asc())
        ).all()
        if req_ids
        else []
    )
    basket_ids = [x.basket_id for x in basket_links]
    baskets = {b.id: b for b in db.scalars(select(CourseBasket).where(CourseBasket.id.in_(basket_ids))).all()} if basket_ids else {}
    basket_items = (
        db.scalars(
            select(CourseBasketItem)
            .where(CourseBasketItem.basket_id.in_(basket_ids))
            .order_by(CourseBasketItem.basket_id.asc(), CourseBasketItem.sort_order.asc())
        ).all()
        if basket_ids
        else []
    )
    items_by_basket: dict[str, list[dict]] = {}
    for item in basket_items:
        course = db.get(Course, item.course_id)
        items_by_basket.setdefault(item.basket_id, []).append(
            {
                "id": item.id,
                "course_id": item.course_id,
                "course_number": course.course_number if course else None,
                "course_title": course.title if course else None,
                "sort_order": item.sort_order,
            }
        )
    baskets_by_req: dict[str, list[dict]] = {}
    for row in basket_links:
        b = baskets.get(row.basket_id)
        baskets_by_req.setdefault(row.requirement_id, []).append(
            {
                "id": row.id,
                "requirement_id": row.requirement_id,
                "basket_id": row.basket_id,
                "basket_name": b.name if b else None,
                "min_count": row.min_count,
                "max_count": row.max_count,
                "sort_order": row.sort_order,
                "courses": items_by_basket.get(row.basket_id, []),
            }
        )
    links_by_req: dict[str, list[dict]] = {}
    for link in links:
        course = db.get(Course, link.course_id)
        links_by_req.setdefault(link.requirement_id, []).append(
            {
                "id": link.id,
                "course_id": link.course_id,
                "course_number": course.course_number if course else None,
                "course_title": course.title if course else None,
                "is_primary": link.is_primary,
            }
        )
    by_parent: dict[Optional[str], list[Requirement]] = {}
    for req in all_reqs:
        by_parent.setdefault(req.parent_requirement_id, []).append(req)
    program_by_id = {p.id: p for p in db.scalars(select(AcademicProgram).where(AcademicProgram.version_id == version_id)).all()}
    code_map = build_program_designer_code_map(all_reqs, program_by_id)

    def build(parent: Optional[str]):
        nodes = []
        for req in by_parent.get(parent, []):
            program = db.get(AcademicProgram, req.program_id) if req.program_id else None
            nodes.append(
                {
                    "id": req.id,
                    "node_code": code_map.get(req.id),
                    "version_id": req.version_id,
                    "name": req.name,
                    "logic_type": req.logic_type,
                    "pick_n": req.pick_n,
                    "sort_order": req.sort_order,
                    "program_id": req.program_id,
                    "program_name": program.name if program else None,
                    "program_type": program.program_type if program else None,
                    "program_division": program.division if program else None,
                    "scope": "CORE" if req.program_id is None else (program.program_type if program else "PROGRAM"),
                    "parent_requirement_id": req.parent_requirement_id,
                    "category": req.category or ("CORE" if req.program_id is None else "MAJOR"),
                    "major_mode": req.major_mode,
                    "track_name": req.track_name,
                    "option_slot_key": req.option_slot_key,
                    "option_slot_capacity": req.option_slot_capacity,
                    "courses": links_by_req.get(req.id, []),
                    "baskets": baskets_by_req.get(req.id, []),
                    "children": build(req.id),
                }
            )
        return nodes

    return {"tree": build(None)}


@app.post("/requirements/fulfillment")
def create_requirement_fulfillment(payload: RequirementFulfillmentIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    required_semester = payload.required_semester
    required_semester_min = payload.required_semester_min
    required_semester_max = payload.required_semester_max
    if (
        required_semester is None
        and required_semester_min is not None
        and required_semester_max is not None
        and required_semester_min == required_semester_max
    ):
        # Normalize equal min/max into a fixed-semester rule.
        required_semester = required_semester_min
        required_semester_min = None
        required_semester_max = None
    if required_semester is not None and (
        required_semester_min is not None or required_semester_max is not None
    ):
        raise HTTPException(
            status_code=400,
            detail="required_semester is mutually exclusive with required_semester_min/required_semester_max",
        )
    if (
        required_semester_min is not None
        and required_semester_max is not None
        and required_semester_min > required_semester_max
    ):
        raise HTTPException(status_code=400, detail="required_semester_min cannot be greater than required_semester_max")
    if required_semester is not None:
        if required_semester_min is not None and required_semester < required_semester_min:
            raise HTTPException(status_code=400, detail="required_semester must be >= required_semester_min")
        if required_semester_max is not None and required_semester > required_semester_max:
            raise HTTPException(status_code=400, detail="required_semester must be <= required_semester_max")
    existing = db.scalar(
        select(RequirementFulfillment).where(
            RequirementFulfillment.requirement_id == payload.requirement_id,
            RequirementFulfillment.course_id == payload.course_id,
        )
    )
    if existing:
        existing.is_primary = payload.is_primary
        existing.required_semester = required_semester
        existing.required_semester_min = required_semester_min
        existing.required_semester_max = required_semester_max
        db.commit()
        db.refresh(existing)
        return serialize(existing)
    current_orders = db.scalars(
        select(RequirementFulfillment.sort_order).where(RequirementFulfillment.requirement_id == payload.requirement_id)
    ).all()
    payload_dict = payload.model_dump()
    payload_dict["required_semester"] = required_semester
    payload_dict["required_semester_min"] = required_semester_min
    payload_dict["required_semester_max"] = required_semester_max
    obj = RequirementFulfillment(**payload_dict, sort_order=(max(current_orders) + 1 if current_orders else 0))
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return serialize(obj)


@app.get("/requirements/fulfillment/{requirement_id}")
def list_requirement_fulfillment(requirement_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    items = db.scalars(
        select(RequirementFulfillment).where(RequirementFulfillment.requirement_id == requirement_id).order_by(RequirementFulfillment.sort_order.asc())
    ).all()
    out = []
    for item in items:
        row = serialize(item)
        course = db.get(Course, item.course_id)
        row["course_number"] = course.course_number if course else None
        row["course_title"] = course.title if course else None
        out.append(row)
    return out


@app.post("/requirements/fulfillment/reorder")
def reorder_requirement_fulfillment(
    payload: list[RequirementFulfillmentOrderIn], db: Session = Depends(get_db), _: User = Depends(require_design)
):
    updated = 0
    for item in payload:
        row = db.get(RequirementFulfillment, item.fulfillment_id)
        if row:
            if item.requirement_id is not None:
                row.requirement_id = item.requirement_id
            row.sort_order = item.sort_order
            updated += 1
    db.commit()
    return {"status": "ok", "updated": updated}


@app.delete("/requirements/fulfillment/{fulfillment_id}")
def delete_requirement_fulfillment(fulfillment_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    item = db.get(RequirementFulfillment, fulfillment_id)
    if not item:
        raise HTTPException(status_code=404, detail="Requirement fulfillment not found")
    db.delete(item)
    db.commit()
    return {"status": "deleted"}


@app.get("/requirements/fulfillment/course/{course_id}")
def list_course_fulfillment(course_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    items = db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.course_id == course_id)).all()
    out = []
    for item in items:
        row = serialize(item)
        req = db.get(Requirement, item.requirement_id)
        row["requirement_name"] = req.name if req else None
        out.append(row)
    return out


@app.get("/requirements/fulfillment/version/{version_id}")
def list_version_fulfillment(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    reqs = db.scalars(select(Requirement).where(Requirement.version_id == version_id)).all()
    req_ids = [r.id for r in reqs]
    if not req_ids:
        return []
    items = db.scalars(
        select(RequirementFulfillment)
        .where(RequirementFulfillment.requirement_id.in_(req_ids))
        .order_by(RequirementFulfillment.requirement_id.asc(), RequirementFulfillment.sort_order.asc())
    ).all()
    out = []
    req_map = {r.id: r for r in reqs}
    for item in items:
        row = serialize(item)
        course = db.get(Course, item.course_id)
        req = req_map.get(item.requirement_id)
        row["course_number"] = course.course_number if course else None
        row["course_title"] = course.title if course else None
        row["requirement_name"] = req.name if req else None
        out.append(row)
    return out


@app.post("/requirements/substitutions")
def create_requirement_substitution(payload: RequirementSubstitutionIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    if payload.primary_course_id == payload.substitute_course_id:
        raise HTTPException(status_code=400, detail="primary_course_id and substitute_course_id must differ")
    existing = db.scalar(
        select(RequirementSubstitution).where(
            RequirementSubstitution.requirement_id == payload.requirement_id,
            RequirementSubstitution.primary_course_id == payload.primary_course_id,
            RequirementSubstitution.substitute_course_id == payload.substitute_course_id,
        )
    )
    if existing:
        return serialize(existing)
    obj = RequirementSubstitution(**payload.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return serialize(obj)


@app.get("/requirements/substitutions/{requirement_id}")
def list_requirement_substitutions(requirement_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    rows = db.scalars(
        select(RequirementSubstitution).where(RequirementSubstitution.requirement_id == requirement_id)
    ).all()
    out = []
    for row in rows:
        item = serialize(row)
        primary = db.get(Course, row.primary_course_id)
        substitute = db.get(Course, row.substitute_course_id)
        item["primary_course_number"] = primary.course_number if primary else None
        item["primary_course_title"] = primary.title if primary else None
        item["substitute_course_number"] = substitute.course_number if substitute else None
        item["substitute_course_title"] = substitute.title if substitute else None
        out.append(item)
    return out


@app.get("/requirements/substitutions/version/{version_id}")
def list_requirement_substitutions_version(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    req_ids = db.scalars(select(Requirement.id).where(Requirement.version_id == version_id)).all()
    if not req_ids:
        return []
    rows = db.scalars(
        select(RequirementSubstitution).where(RequirementSubstitution.requirement_id.in_(req_ids))
    ).all()
    out = []
    for row in rows:
        item = serialize(row)
        primary = db.get(Course, row.primary_course_id)
        substitute = db.get(Course, row.substitute_course_id)
        item["primary_course_number"] = primary.course_number if primary else None
        item["primary_course_title"] = primary.title if primary else None
        item["substitute_course_number"] = substitute.course_number if substitute else None
        item["substitute_course_title"] = substitute.title if substitute else None
        out.append(item)
    return out


@app.put("/requirements/substitutions/{substitution_id}")
def update_requirement_substitution(
    substitution_id: str, payload: RequirementSubstitutionIn, db: Session = Depends(get_db), _: User = Depends(require_design)
):
    row = db.get(RequirementSubstitution, substitution_id)
    if not row:
        raise HTTPException(status_code=404, detail="Requirement substitution not found")
    if payload.primary_course_id == payload.substitute_course_id:
        raise HTTPException(status_code=400, detail="primary_course_id and substitute_course_id must differ")
    row.requirement_id = payload.requirement_id
    row.primary_course_id = payload.primary_course_id
    row.substitute_course_id = payload.substitute_course_id
    row.is_bidirectional = payload.is_bidirectional
    db.commit()
    db.refresh(row)
    return serialize(row)


@app.delete("/requirements/substitutions/{substitution_id}")
def delete_requirement_substitution(substitution_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    row = db.get(RequirementSubstitution, substitution_id)
    if not row:
        raise HTTPException(status_code=404, detail="Requirement substitution not found")
    db.delete(row)
    db.commit()
    return {"status": "deleted"}


@app.get("/design/course-detail/{course_id}")
def design_course_detail(course_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    course = db.get(Course, course_id)
    if not course:
        raise HTTPException(status_code=404, detail="Course not found")
    prereqs = db.scalars(select(CoursePrerequisite).where(CoursePrerequisite.course_id == course_id)).all()
    req_links = db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.course_id == course_id)).all()
    linked_requirements = []
    for link in req_links:
        req = db.get(Requirement, link.requirement_id)
        linked_requirements.append(
            {
                **serialize(link),
                "requirement_name": req.name if req else None,
                "requirement_logic_type": req.logic_type if req else None,
            }
        )
    sections = db.scalars(select(Section).where(Section.course_id == course_id)).all()
    bucket_tags = db.scalars(
        select(CourseBucketTag)
        .where(CourseBucketTag.course_id == course_id)
        .order_by(CourseBucketTag.sort_order.asc(), CourseBucketTag.bucket_code.asc())
    ).all()
    return {
        "general": serialize(course),
        "scheduling": {
            "designated_semester": course.designated_semester,
            "offered_periods_json": course.offered_periods_json,
            "credit_hours": course.credit_hours,
            "min_section_size": course.min_section_size,
        },
        "prerequisites": [serialize(p) for p in prereqs],
        "requirements": linked_requirements,
        "accreditation_buckets": [serialize(b) for b in bucket_tags],
        "resources": [serialize(s) for s in sections],
        "history_hint": "Use /audit and filter entity_type=Course and this course id.",
    }


@app.post("/instructors")
def create_instructor(payload: InstructorIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    obj = Instructor(**payload.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return serialize(obj)


@app.get("/instructors")
def list_instructors(db: Session = Depends(get_db), _: User = Depends(current_user)):
    return [serialize(i) for i in db.scalars(select(Instructor).order_by(Instructor.name.asc())).all()]


@app.post("/instructors/{instructor_id}/qualify/{course_id}")
def qualify_instructor(instructor_id: str, course_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    exists = db.scalar(
        select(InstructorQualification).where(InstructorQualification.instructor_id == instructor_id, InstructorQualification.course_id == course_id)
    )
    if exists:
        return {"status": "exists"}
    obj = InstructorQualification(instructor_id=instructor_id, course_id=course_id)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return serialize(obj)


@app.post("/classrooms")
def create_classroom(payload: ClassroomIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    obj = Classroom(**payload.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return serialize(obj)


@app.get("/classrooms")
def list_classrooms(db: Session = Depends(get_db), _: User = Depends(current_user)):
    return [serialize(r) for r in db.scalars(select(Classroom).order_by(Classroom.building.asc(), Classroom.room_number.asc())).all()]


@app.post("/sections")
def create_section(payload: SectionIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    obj = Section(**payload.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return serialize(obj)


@app.get("/sections")
def list_sections(version_id: Optional[str] = None, db: Session = Depends(get_db), _: User = Depends(current_user)):
    stmt = select(Section)
    if version_id:
        stmt = stmt.where(Section.version_id == version_id)
    return [serialize(s) for s in db.scalars(stmt.order_by(Section.semester_label.asc())).all()]


@app.post("/cadets")
def create_cadet(payload: CadetIn, db: Session = Depends(get_db), user: User = Depends(current_user)):
    if user.role not in {"DESIGN", "ADVISOR"}:
        raise HTTPException(status_code=403, detail="DESIGN or ADVISOR role required")
    obj = Cadet(**payload.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return serialize(obj)


@app.get("/cadets")
def list_cadets(class_year: Optional[int] = None, db: Session = Depends(get_db), _: User = Depends(current_user)):
    stmt = select(Cadet)
    if class_year:
        stmt = stmt.where(Cadet.class_year == class_year)
    return [serialize(c) for c in db.scalars(stmt.order_by(Cadet.class_year.asc(), Cadet.name.asc())).all()]


@app.post("/records")
def create_record(payload: CadetRecordIn, db: Session = Depends(get_db), user: User = Depends(current_user)):
    if user.role not in {"DESIGN", "ADVISOR"}:
        raise HTTPException(status_code=403, detail="DESIGN or ADVISOR role required")
    obj = CadetRecord(**payload.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return serialize(obj)


@app.get("/records")
def list_records(cadet_id: Optional[str] = None, db: Session = Depends(get_db), _: User = Depends(current_user)):
    stmt = select(CadetRecord)
    if cadet_id:
        stmt = stmt.where(CadetRecord.cadet_id == cadet_id)
    return [serialize(r) for r in db.scalars(stmt.order_by(CadetRecord.semester_label.asc())).all()]


def build_canvas_sequence_payload(version_id: str, db: Session) -> dict:
    version = db.get(CurriculumVersion, version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")
    items = db.scalars(
        select(PlanItem).where(PlanItem.version_id == version_id).order_by(PlanItem.semester_index.asc(), PlanItem.position.asc())
    ).all()
    program_by_id = {p.id: p for p in db.scalars(select(AcademicProgram).where(AcademicProgram.version_id == version_id)).all()}
    rows = []
    for item in items:
        c = db.get(Course, item.course_id)
        if not c:
            continue
        prog = program_by_id.get(item.major_program_id) if item.major_program_id else None
        rows.append(
            {
                "semester_index": item.semester_index,
                "position": item.position,
                "course_id": c.id,
                "course_number": c.course_number,
                "aspect": item.aspect,
                "major_program_id": item.major_program_id,
                "major_program_name": prog.name if prog else None,
                "track_name": item.track_name,
            }
        )
    return {
        "version_id": version.id,
        "version_name": version.name,
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "items": rows,
    }


def apply_canvas_sequence_import(version_id: str, payload: CanvasSequenceImportIn, db: Session) -> dict:
    version = db.get(CurriculumVersion, version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    course_by_id = {c.id: c for c in courses}
    course_id_by_number = {normalize_course_number(c.course_number): c.id for c in courses}
    programs = db.scalars(select(AcademicProgram).where(AcademicProgram.version_id == version_id)).all()
    program_by_id = {p.id: p for p in programs}
    program_id_by_name = {str(p.name or "").strip().lower(): p.id for p in programs}

    if payload.replace_existing:
        for row in db.scalars(select(PlanItem).where(PlanItem.version_id == version_id)).all():
            db.delete(row)
        db.flush()

    created = 0
    skipped = 0
    touched_semesters: set[int] = set()
    ordered = sorted(payload.items, key=lambda x: (int(x.semester_index), int(x.position if x.position is not None else 1_000_000)))
    semester_positions: dict[int, int] = {}
    for row in ordered:
        course_id = None
        if row.course_id and row.course_id in course_by_id:
            course_id = row.course_id
        elif row.course_number:
            course_id = course_id_by_number.get(normalize_course_number(row.course_number))
        if not course_id:
            skipped += 1
            continue

        major_program_id = None
        if row.major_program_id and row.major_program_id in program_by_id:
            major_program_id = row.major_program_id
        elif row.major_program_name:
            major_program_id = program_id_by_name.get(str(row.major_program_name).strip().lower())

        aspect = str(row.aspect or "CORE").upper().strip()
        if aspect not in {"CORE", "MAJOR", "TRACK", "PE", "MAJOR_REQUIRED", "MAJOR_TRACK"}:
            aspect = "CORE"

        sem = int(row.semester_index)
        if row.position is None:
            pos = semester_positions.get(sem, 0)
        else:
            pos = max(0, int(row.position))
        semester_positions[sem] = pos + 1
        touched_semesters.add(sem)

        db.add(
            PlanItem(
                version_id=version_id,
                semester_index=sem,
                course_id=course_id,
                position=pos,
                aspect=aspect,
                major_program_id=major_program_id,
                track_name=(str(row.track_name).strip()[:120] if row.track_name else None),
            )
        )
        created += 1

    # Keep contiguous positions within each touched period.
    for sem in touched_semesters:
        rows = db.scalars(
            select(PlanItem).where(PlanItem.version_id == version_id, PlanItem.semester_index == sem).order_by(PlanItem.position.asc(), PlanItem.id.asc())
        ).all()
        for idx, r in enumerate(rows):
            r.position = idx

    db.commit()
    return {"created": created, "skipped": skipped, "replaced_existing": payload.replace_existing}


DATASET_MODULE_ORDER = ("COURSES", "RULES", "CANVAS", "REPORTS")


def normalize_dataset_modules(raw_modules: Optional[list[str] | str]) -> list[str]:
    if raw_modules is None:
        return list(DATASET_MODULE_ORDER)
    if isinstance(raw_modules, str):
        tokens = [x.strip().upper() for x in raw_modules.split(",") if x.strip()]
    else:
        tokens = [str(x).strip().upper() for x in raw_modules if str(x).strip()]
    if not tokens or "ALL" in tokens:
        return list(DATASET_MODULE_ORDER)
    out = [m for m in DATASET_MODULE_ORDER if m in set(tokens)]
    if not out:
        raise HTTPException(status_code=400, detail="modules must include ALL or any of COURSES,RULES,CANVAS,REPORTS")
    return out


def stable_hash(payload: dict | list) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def model_columns(model) -> set[str]:
    return {c.key for c in inspect(model).mapper.column_attrs}


def filter_model_row(model, row: dict) -> dict:
    cols = model_columns(model)
    return {k: row.get(k) for k in row.keys() if k in cols}


def build_course_definitions_payload(version_id: str, db: Session) -> dict:
    courses = db.scalars(select(Course).where(Course.version_id == version_id).order_by(Course.course_number.asc(), Course.id.asc())).all()
    course_rows = [serialize(c) for c in courses]
    course_ids = {c["id"] for c in course_rows}
    prereq_rows = [
        serialize(p)
        for p in db.scalars(select(CoursePrerequisite).order_by(CoursePrerequisite.course_id.asc(), CoursePrerequisite.required_course_id.asc())).all()
        if p.course_id in course_ids or p.required_course_id in course_ids
    ]
    substitution_rows = [
        serialize(s)
        for s in db.scalars(select(CourseSubstitution).order_by(CourseSubstitution.original_course_id.asc(), CourseSubstitution.substitute_course_id.asc())).all()
        if s.original_course_id in course_ids or s.substitute_course_id in course_ids
    ]
    bucket_rows = [
        serialize(b)
        for b in db.scalars(select(CourseBucketTag).order_by(CourseBucketTag.bucket_code.asc(), CourseBucketTag.sort_order.asc())).all()
        if b.course_id in course_ids
    ]
    return {
        "courses": course_rows,
        "course_prerequisites": prereq_rows,
        "course_substitutions": substitution_rows,
        "course_bucket_tags": bucket_rows,
    }


def build_rule_sets_payload(version_id: str, db: Session) -> dict:
    programs = [serialize(p) for p in db.scalars(select(AcademicProgram).where(AcademicProgram.version_id == version_id).order_by(AcademicProgram.name.asc())).all()]
    requirements = [serialize(r) for r in db.scalars(select(Requirement).where(Requirement.version_id == version_id).order_by(Requirement.sort_order.asc(), Requirement.name.asc())).all()]
    req_ids = {r["id"] for r in requirements}
    baskets = [serialize(b) for b in db.scalars(select(CourseBasket).where(CourseBasket.version_id == version_id).order_by(CourseBasket.sort_order.asc(), CourseBasket.name.asc())).all()]
    basket_ids = {b["id"] for b in baskets}
    basket_items = [
        serialize(i)
        for i in db.scalars(select(CourseBasketItem).order_by(CourseBasketItem.basket_id.asc(), CourseBasketItem.sort_order.asc())).all()
        if i.basket_id in basket_ids
    ]
    requirement_baskets = [
        serialize(x)
        for x in db.scalars(select(RequirementBasketLink).order_by(RequirementBasketLink.requirement_id.asc(), RequirementBasketLink.sort_order.asc())).all()
        if x.requirement_id in req_ids and x.basket_id in basket_ids
    ]
    fulfillment = [
        serialize(f)
        for f in db.scalars(select(RequirementFulfillment).order_by(RequirementFulfillment.requirement_id.asc(), RequirementFulfillment.sort_order.asc())).all()
        if f.requirement_id in req_ids
    ]
    req_substitutions = [
        serialize(s)
        for s in db.scalars(select(RequirementSubstitution).order_by(RequirementSubstitution.requirement_id.asc())).all()
        if s.requirement_id in req_ids
    ]
    validation_rules = [serialize(v) for v in db.scalars(select(ValidationRule).order_by(ValidationRule.tier.asc(), ValidationRule.name.asc())).all()]
    return {
        "academic_programs": programs,
        "requirements": requirements,
        "course_baskets": baskets,
        "course_basket_items": basket_items,
        "requirement_basket_links": requirement_baskets,
        "requirement_fulfillment": fulfillment,
        "requirement_substitutions": req_substitutions,
        "validation_rules": validation_rules,
    }


def build_canvas_payload(version_id: str, db: Session) -> dict:
    return build_canvas_sequence_payload(version_id, db)


def build_report_results_payload(version_id: str, db: Session) -> dict:
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "validation": validate(version_id, db, None),
        "feasibility": design_feasibility(version_id, db, None),
        "checklist_core_only": design_checklist(version_id, None, True, db, None),
    }


def compute_dataset_module_ids(version_id: str, db: Session) -> dict:
    course_payload = build_course_definitions_payload(version_id, db)
    rules_payload = build_rule_sets_payload(version_id, db)
    canvas_payload = build_canvas_payload(version_id, db)
    return {
        "courses_id": stable_hash(course_payload),
        "rules_id": stable_hash(rules_payload),
        "canvas_id": stable_hash(canvas_payload),
    }


def build_dataset_bundle(version_id: str, db: Session, modules: Optional[list[str] | str] = None) -> dict:
    version = db.get(CurriculumVersion, version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")
    selected = normalize_dataset_modules(modules)
    ids = compute_dataset_module_ids(version_id, db)
    payload: dict = {}
    if "COURSES" in selected:
        payload["courses"] = build_course_definitions_payload(version_id, db)
    if "RULES" in selected:
        payload["rules"] = build_rule_sets_payload(version_id, db)
    if "CANVAS" in selected:
        payload["canvas"] = build_canvas_payload(version_id, db)
    reports_id = None
    if "REPORTS" in selected:
        report_payload = build_report_results_payload(version_id, db)
        payload["reports"] = report_payload
        reports_id = stable_hash(report_payload)
    return {
        "bundle_id": str(uuid.uuid4()),
        "version_id": version.id,
        "version_name": version.name,
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "modules": selected,
        "module_ids": {
            **ids,
            "reports_id": reports_id,
        },
        "dependencies": {
            "canvas": {"courses_id": ids["courses_id"], "rules_id": ids["rules_id"]},
            "reports": {"courses_id": ids["courses_id"], "rules_id": ids["rules_id"], "canvas_id": ids["canvas_id"]},
        },
        "payload": payload,
    }


def apply_course_definitions_import(version_id: str, course_payload: dict, replace_existing: bool, db: Session) -> dict:
    incoming_courses = course_payload.get("courses") or []
    incoming_prereqs = course_payload.get("course_prerequisites") or []
    incoming_subs = course_payload.get("course_substitutions") or []
    incoming_buckets = course_payload.get("course_bucket_tags") or []

    if replace_existing:
        existing_course_ids = {c.id for c in db.scalars(select(Course).where(Course.version_id == version_id)).all()}
        if existing_course_ids:
            for row in db.scalars(select(PlanItem).where(PlanItem.course_id.in_(existing_course_ids))).all():
                db.delete(row)
            for row in db.scalars(select(CourseBucketTag).where(CourseBucketTag.course_id.in_(existing_course_ids))).all():
                db.delete(row)
            for row in db.scalars(select(CoursePrerequisite)).all():
                if row.course_id in existing_course_ids or row.required_course_id in existing_course_ids:
                    db.delete(row)
            for row in db.scalars(select(CourseSubstitution)).all():
                if row.original_course_id in existing_course_ids or row.substitute_course_id in existing_course_ids:
                    db.delete(row)
            for row in db.scalars(select(Course).where(Course.version_id == version_id)).all():
                db.delete(row)
            db.flush()

    created_courses = 0
    skipped_courses = 0
    for raw in incoming_courses:
        row = filter_model_row(Course, raw)
        row["version_id"] = version_id
        if row.get("id") and db.get(Course, row["id"]):
            skipped_courses += 1
            continue
        db.add(Course(**row))
        created_courses += 1
    db.flush()

    version_course_ids = {c.id for c in db.scalars(select(Course).where(Course.version_id == version_id)).all()}
    created_prereqs = 0
    for raw in incoming_prereqs:
        row = filter_model_row(CoursePrerequisite, raw)
        if row.get("course_id") not in version_course_ids or row.get("required_course_id") not in version_course_ids:
            continue
        if row.get("id") and db.get(CoursePrerequisite, row["id"]):
            continue
        db.add(CoursePrerequisite(**row))
        created_prereqs += 1

    created_subs = 0
    for raw in incoming_subs:
        row = filter_model_row(CourseSubstitution, raw)
        if row.get("original_course_id") not in version_course_ids or row.get("substitute_course_id") not in version_course_ids:
            continue
        if row.get("id") and db.get(CourseSubstitution, row["id"]):
            continue
        db.add(CourseSubstitution(**row))
        created_subs += 1

    created_buckets = 0
    for raw in incoming_buckets:
        row = filter_model_row(CourseBucketTag, raw)
        if row.get("course_id") not in version_course_ids:
            continue
        if row.get("id") and db.get(CourseBucketTag, row["id"]):
            continue
        db.add(CourseBucketTag(**row))
        created_buckets += 1

    db.flush()
    return {
        "courses_created": created_courses,
        "courses_skipped": skipped_courses,
        "prerequisites_created": created_prereqs,
        "substitutions_created": created_subs,
        "bucket_tags_created": created_buckets,
    }


def apply_rule_sets_import(version_id: str, rules_payload: dict, replace_existing: bool, db: Session) -> dict:
    incoming_programs = rules_payload.get("academic_programs") or []
    incoming_requirements = rules_payload.get("requirements") or []
    incoming_baskets = rules_payload.get("course_baskets") or []
    incoming_basket_items = rules_payload.get("course_basket_items") or []
    incoming_req_baskets = rules_payload.get("requirement_basket_links") or []
    incoming_fulfillment = rules_payload.get("requirement_fulfillment") or []
    incoming_req_subs = rules_payload.get("requirement_substitutions") or []
    incoming_validation = rules_payload.get("validation_rules") or []

    if replace_existing:
        req_rows = db.scalars(select(Requirement).where(Requirement.version_id == version_id)).all()
        req_ids = {r.id for r in req_rows}
        basket_rows = db.scalars(select(CourseBasket).where(CourseBasket.version_id == version_id)).all()
        basket_ids = {b.id for b in basket_rows}
        if req_ids:
            for row in db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_(req_ids))).all():
                db.delete(row)
            for row in db.scalars(select(RequirementSubstitution).where(RequirementSubstitution.requirement_id.in_(req_ids))).all():
                db.delete(row)
            for row in db.scalars(select(RequirementBasketLink).where(RequirementBasketLink.requirement_id.in_(req_ids))).all():
                db.delete(row)
        if basket_ids:
            for row in db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id.in_(basket_ids))).all():
                db.delete(row)
        for row in basket_rows:
            db.delete(row)
        for row in req_rows:
            db.delete(row)
        for row in db.scalars(select(AcademicProgram).where(AcademicProgram.version_id == version_id)).all():
            db.delete(row)
        if incoming_validation:
            for row in db.scalars(select(ValidationRule)).all():
                db.delete(row)
        db.flush()

    created_programs = 0
    for raw in incoming_programs:
        row = filter_model_row(AcademicProgram, raw)
        row["version_id"] = version_id
        if row.get("id") and db.get(AcademicProgram, row["id"]):
            continue
        db.add(AcademicProgram(**row))
        created_programs += 1
    db.flush()

    created_requirements = 0
    for raw in incoming_requirements:
        row = filter_model_row(Requirement, raw)
        row["version_id"] = version_id
        if row.get("id") and db.get(Requirement, row["id"]):
            continue
        db.add(Requirement(**row))
        created_requirements += 1
    db.flush()

    req_ids = {r.id for r in db.scalars(select(Requirement).where(Requirement.version_id == version_id)).all()}
    course_ids = {c.id for c in db.scalars(select(Course).where(Course.version_id == version_id)).all()}
    created_baskets = 0
    for raw in incoming_baskets:
        row = filter_model_row(CourseBasket, raw)
        row["version_id"] = version_id
        if row.get("id") and db.get(CourseBasket, row["id"]):
            continue
        db.add(CourseBasket(**row))
        created_baskets += 1
    db.flush()
    basket_ids = {b.id for b in db.scalars(select(CourseBasket).where(CourseBasket.version_id == version_id)).all()}

    created_basket_items = 0
    for raw in incoming_basket_items:
        row = filter_model_row(CourseBasketItem, raw)
        if row.get("basket_id") not in basket_ids or row.get("course_id") not in course_ids:
            continue
        if row.get("id") and db.get(CourseBasketItem, row["id"]):
            continue
        db.add(CourseBasketItem(**row))
        created_basket_items += 1

    created_req_baskets = 0
    for raw in incoming_req_baskets:
        row = filter_model_row(RequirementBasketLink, raw)
        if row.get("requirement_id") not in req_ids or row.get("basket_id") not in basket_ids:
            continue
        if row.get("id") and db.get(RequirementBasketLink, row["id"]):
            continue
        db.add(RequirementBasketLink(**row))
        created_req_baskets += 1

    created_fulfillment = 0
    for raw in incoming_fulfillment:
        row = filter_model_row(RequirementFulfillment, raw)
        if row.get("requirement_id") not in req_ids or row.get("course_id") not in course_ids:
            continue
        if row.get("id") and db.get(RequirementFulfillment, row["id"]):
            continue
        db.add(RequirementFulfillment(**row))
        created_fulfillment += 1

    created_req_subs = 0
    for raw in incoming_req_subs:
        row = filter_model_row(RequirementSubstitution, raw)
        if (
            row.get("requirement_id") not in req_ids
            or row.get("primary_course_id") not in course_ids
            or row.get("substitute_course_id") not in course_ids
        ):
            continue
        if row.get("id") and db.get(RequirementSubstitution, row["id"]):
            continue
        db.add(RequirementSubstitution(**row))
        created_req_subs += 1

    created_validation = 0
    for raw in incoming_validation:
        row = filter_model_row(ValidationRule, raw)
        if not row.get("name"):
            continue
        if db.scalar(select(ValidationRule).where(ValidationRule.name == row["name"])):
            continue
        if row.get("id") and db.get(ValidationRule, row["id"]):
            row.pop("id", None)
        db.add(ValidationRule(**row))
        created_validation += 1

    db.flush()
    return {
        "programs_created": created_programs,
        "requirements_created": created_requirements,
        "course_baskets_created": created_baskets,
        "course_basket_items_created": created_basket_items,
        "requirement_basket_links_created": created_req_baskets,
        "requirement_fulfillment_created": created_fulfillment,
        "requirement_substitutions_created": created_req_subs,
        "validation_rules_created": created_validation,
    }


def dataset_import_mismatch_report(modules: list[str], bundle: dict, current_ids: dict) -> list[str]:
    incoming_ids = bundle.get("module_ids") or {}
    deps = bundle.get("dependencies") or {}

    def effective_id(kind: str) -> Optional[str]:
        key = f"{kind}_id"
        if kind.upper() in modules and incoming_ids.get(key):
            return incoming_ids.get(key)
        return current_ids.get(key)

    mismatches: list[str] = []
    if "CANVAS" in modules:
        canvas_dep = deps.get("canvas") or {}
        expected_courses = canvas_dep.get("courses_id")
        expected_rules = canvas_dep.get("rules_id")
        if expected_courses and expected_courses != effective_id("courses"):
            mismatches.append(f"Canvas expects courses_id={expected_courses}, but active/imported courses_id={effective_id('courses')}.")
        if expected_rules and expected_rules != effective_id("rules"):
            mismatches.append(f"Canvas expects rules_id={expected_rules}, but active/imported rules_id={effective_id('rules')}.")
    if "REPORTS" in modules:
        report_dep = deps.get("reports") or {}
        for k in ("courses", "rules", "canvas"):
            expected = report_dep.get(f"{k}_id")
            actual = effective_id(k)
            if expected and expected != actual:
                mismatches.append(f"Reports expect {k}_id={expected}, but active/imported {k}_id={actual}.")
    return mismatches


def apply_dataset_bundle_import(version_id: str, bundle: dict, db: Session, modules: Optional[list[str] | str] = None, replace_existing: bool = True) -> dict:
    selected = normalize_dataset_modules(modules if modules is not None else bundle.get("modules"))
    payload = bundle.get("payload") or {}
    current_ids = compute_dataset_module_ids(version_id, db)
    mismatches = dataset_import_mismatch_report(selected, bundle, current_ids)
    applied: dict = {}

    if "COURSES" in selected:
        applied["courses"] = apply_course_definitions_import(version_id, payload.get("courses") or {}, replace_existing, db)
    if "RULES" in selected:
        applied["rules"] = apply_rule_sets_import(version_id, payload.get("rules") or {}, replace_existing, db)
    if "CANVAS" in selected:
        canvas_payload = payload.get("canvas") or {}
        canvas_in = CanvasSequenceImportIn(
            name=str(bundle.get("name") or "Imported Canvas"),
            replace_existing=replace_existing,
            items=[CanvasSequenceItemIn(**x) for x in (canvas_payload.get("items") or [])],
        )
        applied["canvas"] = apply_canvas_sequence_import(version_id, canvas_in, db)
    if "REPORTS" in selected:
        # Report rows are derived outputs; importing preserves archival context in the bundle itself.
        applied["reports"] = {"status": "accepted", "result_count": len((payload.get("reports") or {}).keys())}

    db.commit()
    ids_after = compute_dataset_module_ids(version_id, db)
    return {
        "modules": selected,
        "replace_existing": replace_existing,
        "mismatches": mismatches,
        "applied": applied,
        "ids_before": current_ids,
        "ids_after": ids_after,
    }


@app.get("/design/canvas/{version_id}")
def canvas(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    out = {str(i): [] for i in ALL_PLAN_PERIODS}
    items = db.scalars(select(PlanItem).where(PlanItem.version_id == version_id).order_by(PlanItem.semester_index, PlanItem.position)).all()
    for item in items:
        c = db.get(Course, item.course_id)
        if c:
            major_name = None
            if item.major_program_id:
                prog = db.get(AcademicProgram, item.major_program_id)
                major_name = prog.name if prog else None
            out[str(item.semester_index)].append(
                {
                    "plan_item_id": item.id,
                    "course_id": c.id,
                    "course_number": c.course_number,
                    "title": c.title,
                    "credits": c.credit_hours,
                    "aspect": item.aspect,
                    "major_program_id": item.major_program_id,
                    "major_program_name": major_name,
                    "track_name": item.track_name,
                }
            )
    return out


@app.get("/design/canvas/{version_id}/view")
def canvas_view(
    version_id: str,
    mode: str = Query("GENERIC_CORE"),
    program_id: Optional[str] = None,
    compare_version_id: Optional[str] = None,
    db: Session = Depends(get_db),
    _: User = Depends(current_user),
):
    base_canvas = canvas(version_id, db, _)
    mode = mode.upper()
    if mode == "GENERIC_CORE":
        return {"mode": mode, "canvas": base_canvas}
    if mode == "MAJOR_SPECIFIC":
        if not program_id:
            return {"mode": mode, "canvas": base_canvas, "note": "program_id is required for major-specific filtering"}
        req_ids = {r.id for r in db.scalars(select(Requirement).where(Requirement.program_id == program_id)).all()}
        course_ids = {rf.course_id for rf in db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_(req_ids))).all()} if req_ids else set()
        filtered = {k: [c for c in v if c["course_id"] in course_ids] for k, v in base_canvas.items()}
        return {"mode": mode, "program_id": program_id, "canvas": filtered}
    if mode == "WING_AGGREGATE":
        totals = {k: len(v) for k, v in base_canvas.items()}
        credit = {k: sum(float(x["credits"]) for x in v) for k, v in base_canvas.items()}
        return {"mode": mode, "aggregate_courses_by_semester": totals, "aggregate_credits_by_semester": credit}
    if mode == "COHORT_TRANSITION":
        cohorts = [serialize(c) for c in db.scalars(select(CohortAssignment).order_by(CohortAssignment.class_year.asc())).all()]
        return {"mode": mode, "canvas": base_canvas, "cohort_assignments": cohorts}
    if mode == "COMPARISON":
        if not compare_version_id:
            return {"mode": mode, "canvas": base_canvas, "note": "compare_version_id is required for comparison"}
        diff = detailed_diff(version_id, compare_version_id, db, _)
        return {"mode": mode, "canvas": base_canvas, "diff": diff}
    return {"mode": mode, "canvas": base_canvas, "note": "unknown mode"}


@app.post("/design/canvas/add")
def canvas_add(
    version_id: str,
    course_id: str,
    semester_index: int = Query(..., ge=0, le=MAX_PLAN_PERIOD),
    aspect: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    major_mode: Optional[str] = Query(None),
    major_program_id: Optional[str] = Query(None),
    track_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(require_design),
):
    if aspect is None:
        cat = (category or "CORE").upper()
        if cat not in {"CORE", "MAJOR", "PE"}:
            raise HTTPException(status_code=400, detail="category must be one of CORE, MAJOR, PE")
        if cat == "CORE":
            aspect = "CORE"
        elif cat == "PE":
            aspect = "PE"
        else:
            mode = (major_mode or "REQUIREMENT").upper()
            if mode not in {"REQUIREMENT", "TRACK"}:
                raise HTTPException(status_code=400, detail="major_mode must be REQUIREMENT or TRACK")
            aspect = "MAJOR_REQUIRED" if mode == "REQUIREMENT" else "MAJOR_TRACK"
            if mode == "TRACK" and not (track_name or "").strip():
                raise HTTPException(status_code=400, detail="track_name is required when major_mode=TRACK")
    else:
        aspect = aspect.upper()
        if aspect not in {"CORE", "MAJOR", "TRACK", "PE", "MAJOR_REQUIRED", "MAJOR_TRACK"}:
            raise HTTPException(
                status_code=400,
                detail="aspect must be one of CORE, MAJOR, TRACK, PE, MAJOR_REQUIRED, MAJOR_TRACK",
            )

    max_pos = db.scalars(select(PlanItem.position).where(PlanItem.version_id == version_id, PlanItem.semester_index == semester_index)).all()
    item = PlanItem(
        version_id=version_id,
        course_id=course_id,
        semester_index=semester_index,
        position=(max(max_pos) + 1 if max_pos else 0),
        aspect=aspect,
        major_program_id=major_program_id,
        track_name=(track_name.strip()[:120] if track_name else None),
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return serialize(item)


@app.post("/design/canvas/move")
def canvas_move(payload: MoveIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    item = db.get(PlanItem, payload.plan_item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Plan item not found")
    source_semester = item.semester_index
    target_semester = payload.target_semester

    if source_semester == target_semester:
        siblings = db.scalars(
            select(PlanItem)
            .where(PlanItem.version_id == item.version_id, PlanItem.semester_index == source_semester)
            .order_by(PlanItem.position.asc(), PlanItem.id.asc())
        ).all()
        ordered = [s for s in siblings if s.id != item.id]
        insert_at = max(0, min(payload.target_position, len(ordered)))
        ordered.insert(insert_at, item)
        for idx, row in enumerate(ordered):
            row.position = idx
    else:
        source_rows = db.scalars(
            select(PlanItem)
            .where(PlanItem.version_id == item.version_id, PlanItem.semester_index == source_semester, PlanItem.id != item.id)
            .order_by(PlanItem.position.asc(), PlanItem.id.asc())
        ).all()
        target_rows = db.scalars(
            select(PlanItem)
            .where(PlanItem.version_id == item.version_id, PlanItem.semester_index == target_semester)
            .order_by(PlanItem.position.asc(), PlanItem.id.asc())
        ).all()
        insert_at = max(0, min(payload.target_position, len(target_rows)))
        target_rows.insert(insert_at, item)
        for idx, row in enumerate(source_rows):
            row.position = idx
        for idx, row in enumerate(target_rows):
            row.semester_index = target_semester
            row.position = idx
    db.commit()
    return {"status": "moved"}


@app.put("/design/canvas/{plan_item_id}")
def canvas_update(plan_item_id: str, payload: CanvasItemUpdateIn, db: Session = Depends(get_db), _: User = Depends(require_design)):
    item = db.get(PlanItem, plan_item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Plan item not found")

    if payload.semester_index is not None:
        item.semester_index = payload.semester_index

    # Resolve classification model; keep backward compatibility with direct aspect updates.
    resolved_aspect = None
    resolved_major_program_id = item.major_program_id
    resolved_track_name = item.track_name

    if payload.aspect is not None:
        a = payload.aspect.upper()
        if a not in {"CORE", "MAJOR", "TRACK", "PE", "MAJOR_REQUIRED", "MAJOR_TRACK"}:
            raise HTTPException(
                status_code=400,
                detail="aspect must be one of CORE, MAJOR, TRACK, PE, MAJOR_REQUIRED, MAJOR_TRACK",
            )
        resolved_aspect = a
    elif payload.category is not None:
        cat = payload.category.upper()
        if cat not in {"CORE", "MAJOR", "PE"}:
            raise HTTPException(status_code=400, detail="category must be one of CORE, MAJOR, PE")
        if cat == "CORE":
            resolved_aspect = "CORE"
            resolved_major_program_id = None
            resolved_track_name = None
        elif cat == "PE":
            resolved_aspect = "PE"
            resolved_major_program_id = None
            resolved_track_name = None
        else:
            mode = (payload.major_mode or "REQUIREMENT").upper()
            if mode not in {"REQUIREMENT", "TRACK"}:
                raise HTTPException(status_code=400, detail="major_mode must be REQUIREMENT or TRACK")
            resolved_aspect = "MAJOR_REQUIRED" if mode == "REQUIREMENT" else "MAJOR_TRACK"
            resolved_major_program_id = payload.major_program_id
            if mode == "TRACK":
                if not (payload.track_name or "").strip():
                    raise HTTPException(status_code=400, detail="track_name is required when major_mode=TRACK")
                resolved_track_name = payload.track_name.strip()[:120]
            else:
                resolved_track_name = None

    if payload.major_program_id is not None and payload.category is None:
        resolved_major_program_id = payload.major_program_id
    if payload.track_name is not None and payload.category is None:
        resolved_track_name = payload.track_name.strip()[:120] if payload.track_name else None

    if resolved_aspect is not None:
        item.aspect = resolved_aspect
    item.major_program_id = resolved_major_program_id
    item.track_name = resolved_track_name

    db.commit()
    db.refresh(item)
    return serialize(item)


@app.delete("/design/canvas/{plan_item_id}")
def canvas_delete(plan_item_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    item = db.get(PlanItem, plan_item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Plan item not found")
    db.delete(item)
    db.commit()
    return {"status": "deleted"}


@app.get("/design/datasets/{version_id}/export")
def export_dataset_bundle(
    version_id: str,
    modules: str = Query("ALL"),
    db: Session = Depends(get_db),
    _: User = Depends(current_user),
):
    return build_dataset_bundle(version_id, db, modules)


@app.post("/design/datasets/{version_id}/import")
def import_dataset_bundle(
    version_id: str,
    payload: dict,
    modules: Optional[str] = None,
    replace_existing: bool = True,
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    bundle = payload.get("bundle") if isinstance(payload, dict) and "bundle" in payload else payload
    if not isinstance(bundle, dict):
        raise HTTPException(status_code=400, detail="Invalid bundle payload")
    result = apply_dataset_bundle_import(version_id, bundle, db, modules=modules, replace_existing=replace_existing)
    write_audit(
        db,
        user,
        "DATASET_IMPORT",
        "CurriculumVersion",
        version_id,
        json.dumps({"modules": result["modules"], "replace_existing": replace_existing, "mismatches": result["mismatches"]}),
    )
    return {"status": "ok", **result}


@app.get("/design/datasets/{version_id}/saved")
def list_saved_dataset_bundles(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    version = db.get(CurriculumVersion, version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")
    rows = db.scalars(
        select(DataBundleSnapshot).where(DataBundleSnapshot.version_id == version_id).order_by(DataBundleSnapshot.created_at.desc())
    ).all()
    return [serialize(r) for r in rows]


@app.post("/design/datasets/{version_id}/save")
def save_dataset_bundle(
    version_id: str,
    name: str = Query(..., min_length=1, max_length=120),
    modules: str = Query("ALL"),
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    bundle = build_dataset_bundle(version_id, db, modules)
    row = DataBundleSnapshot(
        version_id=version_id,
        name=name.strip(),
        modules_csv=",".join(bundle.get("modules") or []),
        bundle_json=json.dumps(bundle),
        created_by=user.id,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    write_audit(
        db,
        user,
        "DATASET_SAVE",
        "DataBundleSnapshot",
        row.id,
        json.dumps({"version_id": version_id, "modules": row.modules_csv, "name": row.name}),
    )
    return serialize(row)


@app.post("/design/datasets/{version_id}/saved/{snapshot_id}/load")
def load_saved_dataset_bundle(
    version_id: str,
    snapshot_id: str,
    replace_existing: bool = True,
    modules: Optional[str] = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    row = db.get(DataBundleSnapshot, snapshot_id)
    if not row or row.version_id != version_id:
        raise HTTPException(status_code=404, detail="Saved dataset bundle not found")
    try:
        bundle = json.loads(row.bundle_json or "{}")
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Saved dataset bundle payload is invalid") from exc
    result = apply_dataset_bundle_import(version_id, bundle, db, modules=modules, replace_existing=replace_existing)
    write_audit(
        db,
        user,
        "DATASET_LOAD",
        "DataBundleSnapshot",
        row.id,
        json.dumps({"version_id": version_id, "modules": result["modules"], "replace_existing": replace_existing}),
    )
    return {"status": "ok", "snapshot_id": row.id, **result}


@app.get("/design/checklist/{version_id}")
def design_checklist(
    version_id: str,
    program_ids: Optional[str] = None,
    include_core: bool = True,
    db: Session = Depends(get_db),
    _: User = Depends(current_user),
):
    selected_program_ids = []
    if program_ids:
        selected_program_ids = [x.strip() for x in program_ids.split(",") if x.strip()]

    canvas_items = db.scalars(select(PlanItem).where(PlanItem.version_id == version_id)).all()
    planned_course_ids = {x.course_id for x in canvas_items}
    planned_course_semesters: dict[str, set[int]] = {}
    for x in canvas_items:
        planned_course_semesters.setdefault(x.course_id, set()).add(x.semester_index)

    reqs = db.scalars(select(Requirement).where(Requirement.version_id == version_id).order_by(Requirement.sort_order.asc())).all()
    req_by_id = {r.id: r for r in reqs}
    program_by_id = {p.id: p for p in db.scalars(select(AcademicProgram).where(AcademicProgram.version_id == version_id)).all()}
    node_code_map = build_program_designer_code_map(reqs, program_by_id)
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    course_by_id = {c.id: c for c in courses}
    course_num_by_id = {c.id: c.course_number for c in courses}
    course_id_by_number = {normalize_course_number(c.course_number): c.id for c in courses}
    planned_credit_hours = 0.0
    for item in canvas_items:
        c = course_by_id.get(item.course_id)
        if c:
            planned_credit_hours += float(c.credit_hours or 0.0)
    planned_academic_semesters = sorted({x.semester_index for x in canvas_items if x.semester_index in ACADEMIC_PERIODS})
    child_map: dict[Optional[str], list[Requirement]] = {}
    for r in reqs:
        child_map.setdefault(r.parent_requirement_id, []).append(r)

    target_reqs = []
    for r in reqs:
        if r.parent_requirement_id is not None:
            continue
        if r.program_id is None and include_core:
            target_reqs.append(r)
        elif r.program_id in selected_program_ids:
            target_reqs.append(r)

    direct_links = db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_([r.id for r in reqs]))).all() if reqs else []
    links_by_req: dict[str, list[RequirementFulfillment]] = {}
    for link in direct_links:
        links_by_req.setdefault(link.requirement_id, []).append(link)
    req_basket_links = (
        db.scalars(select(RequirementBasketLink).where(RequirementBasketLink.requirement_id.in_([r.id for r in reqs]))).all() if reqs else []
    )
    basket_ids = [x.basket_id for x in req_basket_links]
    basket_items = (
        db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id.in_(basket_ids))).all() if basket_ids else []
    )
    basket_course_ids_by_basket: dict[str, list[str]] = {}
    for item in basket_items:
        basket_course_ids_by_basket.setdefault(item.basket_id, []).append(item.course_id)
    baskets_by_req: dict[str, list[RequirementBasketLink]] = {}
    for bl in req_basket_links:
        baskets_by_req.setdefault(bl.requirement_id, []).append(bl)
    req_sub_rows = (
        db.scalars(select(RequirementSubstitution).where(RequirementSubstitution.requirement_id.in_([r.id for r in reqs]))).all() if reqs else []
    )
    req_sub_map: dict[str, dict[str, set[str]]] = {}
    for row in req_sub_rows:
        m = req_sub_map.setdefault(row.requirement_id, {})
        m.setdefault(row.primary_course_id, set()).add(row.substitute_course_id)
        if row.is_bidirectional:
            m.setdefault(row.substitute_course_id, set()).add(row.primary_course_id)
    req_by_name: dict[str, list[Requirement]] = {}
    req_children: dict[Optional[str], list[Requirement]] = {}
    for r in reqs:
        req_by_name.setdefault((r.name or "").strip().lower(), []).append(r)
        req_children.setdefault(r.parent_requirement_id, []).append(r)
    direct_links_by_req: dict[str, list[RequirementFulfillment]] = {}
    for link in direct_links:
        direct_links_by_req.setdefault(link.requirement_id, []).append(link)
    collect_cache: dict[str, set[str]] = {}

    def collect_requirement_course_ids(req_id: str) -> set[str]:
        if req_id in collect_cache:
            return set(collect_cache[req_id])
        found: set[str] = set()
        stack = [req_id]
        seen = set()
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            for link in direct_links_by_req.get(cur, []):
                if link.course_id:
                    found.add(link.course_id)
            for bl in baskets_by_req.get(cur, []):
                for cid in basket_course_ids_by_basket.get(bl.basket_id, []):
                    if cid:
                        found.add(cid)
            for child in req_children.get(cur, []):
                stack.append(child.id)
        collect_cache[req_id] = set(found)
        return found

    def gather_planned_program_requirement_courses(program_id: str) -> tuple[set[str], float]:
        top_program_reqs = [r for r in reqs if r.program_id == program_id and r.parent_requirement_id is None]
        out_courses: set[str] = set()
        for tr in top_program_reqs:
            out_courses |= collect_requirement_course_ids(tr.id)
        out_courses = {cid for cid in out_courses if cid in planned_course_ids}
        out_credits = sum(float(course_by_id[cid].credit_hours or 0.0) for cid in out_courses if cid in course_by_id)
        return out_courses, out_credits

    def evaluate(req_id: str) -> dict:
        req = req_by_id[req_id]
        children = child_map.get(req_id, [])
        child_results = [evaluate(c.id) for c in children]
        basket_results = []
        for bl in baskets_by_req.get(req_id, []):
            basket_course_ids = list(dict.fromkeys(basket_course_ids_by_basket.get(bl.basket_id, [])))
            needed = max(1, int(bl.min_count or 1))
            matched = [cid for cid in basket_course_ids if cid in planned_course_ids]
            sat = len(matched) >= needed and len(basket_course_ids) >= needed
            basket_results.append(
                {
                    "requirement_id": f"basket:{bl.id}",
                    "node_code": None,
                    "name": f"Basket ({needed} of {len(basket_course_ids)})",
                    "program_id": req.program_id,
                    "logic_type": "PICK_N",
                    "pick_n": needed,
                    "is_satisfied": sat,
                    "matched_direct_course_count": len(matched),
                    "direct_course_count": len(basket_course_ids),
                    "satisfied_units": len(matched),
                    "required_units": needed,
                    "fixed_semester_violations": [],
                    "children": [],
                }
            )
        child_results = [*child_results, *basket_results]
        links = links_by_req.get(req_id, [])
        link_course_ids = [l.course_id for l in links]
        req_subs = req_sub_map.get(req_id, {})
        matched_course_ids = []
        fixed_semester_violations = []
        for link in links:
            cid = link.course_id
            required_semester = link.required_semester
            required_semester_min = link.required_semester_min
            required_semester_max = link.required_semester_max
            substitutes = req_subs.get(cid, set())
            primary_planned = cid in planned_course_ids
            planned_substitutes = [s for s in substitutes if s in planned_course_ids]
            has_constraints = required_semester is not None or required_semester_min is not None or required_semester_max is not None
            if not has_constraints:
                if primary_planned or planned_substitutes:
                    matched_course_ids.append(cid)
                continue
            def semester_ok(sem: int) -> bool:
                if required_semester is not None and sem != required_semester:
                    return False
                if required_semester_min is not None and sem < required_semester_min:
                    return False
                if required_semester_max is not None and sem > required_semester_max:
                    return False
                return True
            primary_ok = primary_planned and any(semester_ok(s) for s in planned_course_semesters.get(cid, set()))
            substitute_ok = any(any(semester_ok(s) for s in planned_course_semesters.get(sub_id, set())) for sub_id in planned_substitutes)
            if primary_ok or substitute_ok:
                matched_course_ids.append(cid)
                continue
            if primary_planned or planned_substitutes:
                option_ids = [cid, *planned_substitutes]
                option_nums = [course_num_by_id.get(x, x) for x in option_ids]
                parts = []
                if required_semester is not None:
                    parts.append(period_label(required_semester))
                if required_semester_min is not None:
                    parts.append(f"{period_label(required_semester_min)} or later")
                if required_semester_max is not None:
                    parts.append(f"{period_label(required_semester_max)} or earlier")
                window_text = ", ".join(parts) if parts else "required period window"
                fixed_semester_violations.append(
                    f"{' / '.join(option_nums)} must satisfy: {window_text}"
                )
        matched_count = len(matched_course_ids)
        child_pass_count = sum(1 for c in child_results if c["is_satisfied"])
        unit_required = len(link_course_ids) + len(child_results)
        unit_satisfied = matched_count + child_pass_count

        logic = (req.logic_type or "ALL_REQUIRED").upper()
        if logic == "OPTION_SLOT":
            needed = max(1, int(req.option_slot_capacity or req.pick_n or 1))
            is_satisfied = unit_satisfied >= needed
            required_units = needed
        elif logic in {"PICK_N", "ANY_N"}:
            needed = req.pick_n or 1
            is_satisfied = unit_satisfied >= needed
            required_units = needed
        elif logic in {"ANY_ONE", "ONE_OF"}:
            is_satisfied = unit_satisfied >= 1
            required_units = 1
        else:
            # ALL_REQUIRED default
            is_satisfied = unit_satisfied >= unit_required if unit_required > 0 else True
            required_units = unit_required

        return {
            "requirement_id": req.id,
            "node_code": node_code_map.get(req.id),
            "name": req.name,
            "program_id": req.program_id,
            "logic_type": logic,
            "pick_n": req.pick_n,
            "is_satisfied": is_satisfied,
            "matched_direct_course_count": matched_count,
            "direct_course_count": len(link_course_ids),
            "satisfied_units": unit_satisfied,
            "required_units": required_units,
            "fixed_semester_violations": fixed_semester_violations,
            "children": child_results,
        }

    results = [evaluate(r.id) for r in target_reqs]

    # Include active major/minor Core Rules in checklist output for selected programs.
    selected_program_set = set(selected_program_ids)
    selected_programs = (
        db.scalars(select(AcademicProgram).where(AcademicProgram.id.in_(selected_program_ids))).all()
        if selected_program_ids
        else []
    )
    selected_program_by_name = {(p.name or "").strip().lower(): p for p in selected_programs}
    rule_nodes = []
    for rule in db.scalars(select(ValidationRule).where(ValidationRule.active.is_(True))).all():
        try:
            cfg = json.loads(rule.config_json or "{}")
        except Exception:
            cfg = {}
        if is_cadet_performance_rule(cfg):
            continue
        rule_type = str(cfg.get("type") or "").upper()
        if rule_type not in {"MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"}:
            continue
        target_program = None
        pid = cfg.get("program_id")
        pname = str(cfg.get("program_name") or "").strip().lower()
        if pid and pid in selected_program_set:
            target_program = next((p for p in selected_programs if p.id == pid), None)
        elif pname and pname in selected_program_by_name:
            target_program = selected_program_by_name[pname]
        if not target_program:
            continue
        groups = cfg.get("required_core_groups") or []
        children = []
        for idx, g in enumerate(groups):
            g = g or {}
            group_name = str(g.get("name") or f"Core Rule {idx + 1}").strip()
            min_count = max(1, int(g.get("min_count") or 1))
            required_semester = g.get("required_semester")
            required_semester_min = g.get("required_semester_min")
            required_semester_max = g.get("required_semester_max")
            try:
                required_semester = int(required_semester) if required_semester is not None else None
            except Exception:
                required_semester = None
            try:
                required_semester_min = int(required_semester_min) if required_semester_min is not None else None
            except Exception:
                required_semester_min = None
            try:
                required_semester_max = int(required_semester_max) if required_semester_max is not None else None
            except Exception:
                required_semester_max = None
            violations: list[str] = []
            if required_semester is not None and (required_semester_min is not None or required_semester_max is not None):
                violations.append("Fixed semester cannot be combined with no-earlier/no-later.")
            if required_semester_min is not None and required_semester_max is not None and required_semester_min > required_semester_max:
                violations.append("No-earlier semester cannot be later than no-later semester.")
            group_course_ids: set[str] = set()
            for num in g.get("course_numbers") or []:
                cid = course_id_by_number.get(normalize_course_number(str(num)))
                if cid:
                    group_course_ids.add(cid)
            for req_id in g.get("requirement_ids") or []:
                if req_id in req_by_id:
                    group_course_ids |= collect_requirement_course_ids(req_id)
            for req_name in g.get("requirement_names") or []:
                for req in req_by_name.get(str(req_name).strip().lower(), []):
                    group_course_ids |= collect_requirement_course_ids(req.id)
            matched_ids = sorted([cid for cid in group_course_ids if cid in planned_course_ids])
            matched_ids_without_timing = list(matched_ids)
            if required_semester is not None or required_semester_min is not None or required_semester_max is not None:
                def semester_ok(sem: int) -> bool:
                    if required_semester is not None and sem != required_semester:
                        return False
                    if required_semester_min is not None and sem < required_semester_min:
                        return False
                    if required_semester_max is not None and sem > required_semester_max:
                        return False
                    return True
                matched_ids = sorted([cid for cid in matched_ids if any(semester_ok(s) for s in planned_course_semesters.get(cid, set()))])
            if not group_course_ids:
                violations.append("No resolvable courses for this rule.")
            if len(matched_ids) < min_count:
                sem_bits = []
                if required_semester is not None:
                    sem_bits.append(period_short_label(required_semester))
                if required_semester_min is not None:
                    sem_bits.append(f">={period_short_label(required_semester_min)}")
                if required_semester_max is not None:
                    sem_bits.append(f"<={period_short_label(required_semester_max)}")
                sem_suffix = f" [{', '.join(sem_bits)}]" if sem_bits else ""
                if matched_ids_without_timing and not matched_ids:
                    violations.append(f"Choice is present but violates timing rule{sem_suffix}.")
            child_satisfied = len(matched_ids) >= min_count and not violations
            children.append(
                {
                    "requirement_id": f"core-rule-group:{rule.id}:{idx}",
                    "node_code": None,
                    "name": group_name,
                    "program_id": target_program.id,
                    "logic_type": "PICK_N",
                    "pick_n": min_count,
                    "is_satisfied": child_satisfied,
                    "matched_direct_course_count": len(matched_ids),
                    "direct_course_count": len(group_course_ids),
                    "satisfied_units": len(matched_ids),
                    "required_units": min_count,
                    "fixed_semester_violations": violations,
                    "children": [],
                }
            )
        top_satisfied = all(c["is_satisfied"] for c in children) if children else True
        parent_code = next((node_code_map.get(r.id) for r in target_reqs if r.program_id == target_program.id), None)
        core_rule_code = f"{parent_code}.C1" if parent_code else "R1.C1"
        for idx, child in enumerate(children):
            child["node_code"] = f"{core_rule_code}.R{idx + 1}"
        rule_nodes.append(
            {
                "requirement_id": f"core-rule:{rule.id}",
                "node_code": core_rule_code,
                "name": "Core Rules",
                "program_id": target_program.id,
                "logic_type": "ALL_REQUIRED",
                "pick_n": None,
                "is_satisfied": top_satisfied,
                "matched_direct_course_count": 0,
                "direct_course_count": 0,
                "satisfied_units": sum(1 for c in children if c["is_satisfied"]),
                "required_units": len(children),
                "fixed_semester_violations": [],
                "children": children,
            }
        )

    active_rules = active_design_rules_with_config(db)
    residency_hours_rule, residency_hours_cfg = find_active_rule(
        active_rules,
        rule_type="RESIDENCY_MIN_HOURS",
        names=["Residency minimum in-residence hours", "COI residency requirements"],
    )
    residency_sem_rule, residency_sem_cfg = find_active_rule(
        active_rules,
        rule_type="RESIDENCY_MIN_ACADEMIC_SEMESTERS",
        names=["Residency minimum academic semesters", "COI residency requirements"],
    )
    residency_min_hours = DEFAULT_RESIDENCY_MIN_HOURS
    residency_min_academic = DEFAULT_RESIDENCY_MIN_ACADEMIC_SEMESTERS
    try:
        residency_min_hours = float(
            residency_hours_cfg.get("min_hours", residency_hours_cfg.get("minimum_in_residence_hours", residency_min_hours))
        )
    except Exception:
        residency_min_hours = DEFAULT_RESIDENCY_MIN_HOURS
    try:
        residency_min_academic = int(
            residency_sem_cfg.get("min_semesters", residency_sem_cfg.get("minimum_academic_semesters", residency_min_academic))
        )
    except Exception:
        residency_min_academic = DEFAULT_RESIDENCY_MIN_ACADEMIC_SEMESTERS

    all_results = [*results, *rule_nodes]
    total = len(all_results)
    satisfied = sum(1 for r in all_results if r["is_satisfied"])
    # Validation rule line items (same structure used by Program Feasibility and Course-of-Study Feasibility).
    validation_items = []
    residency_hours_status = "PASS" if planned_credit_hours >= residency_min_hours else "FAIL"
    validation_items.append(
        {
            "rule_code": str(residency_hours_rule.rule_code or "").strip() if residency_hours_rule else "",
            "rule_name": residency_hours_rule.name if residency_hours_rule else "Residency minimum in-residence hours",
            "status": residency_hours_status,
            "message": (
                f"Planned in-residence credit hours {planned_credit_hours:.1f}; minimum {residency_min_hours:.0f}."
            ),
        }
    )
    residency_sem_status = "PASS" if len(planned_academic_semesters) >= residency_min_academic else "FAIL"
    validation_items.append(
        {
            "rule_code": str(residency_sem_rule.rule_code or "").strip() if residency_sem_rule else "",
            "rule_name": residency_sem_rule.name if residency_sem_rule else "Residency minimum academic semesters",
            "status": residency_sem_status,
            "message": (
                f"Academic semesters with load {len(planned_academic_semesters)}; minimum {residency_min_academic}."
            ),
        }
    )
    bucket_rows = db.scalars(select(CourseBucketTag).where(CourseBucketTag.course_id.in_(list(planned_course_ids)))).all() if planned_course_ids else []
    validation_items.extend(
        abet_validation_items_for_courses(
            active_rules,
            programs=selected_programs,
            course_ids=set(planned_course_ids),
            bucket_rows=bucket_rows,
            course_by_id=course_by_id,
        )
    )
    pathway_definitions = get_pathway_definitions(active_rules)
    pathway_program_course_sets: dict[str, set[str]] = {}
    pathway_program_credit_sums: dict[str, float] = {}
    for p in selected_programs:
        pcs, pcredits = gather_planned_program_requirement_courses(p.id)
        pathway_program_course_sets[p.id] = set(pcs)
        pathway_program_credit_sums[p.id] = float(pcredits)
    validation_items.extend(
        build_pathway_validation_items(
            rule_rows=active_rules,
            programs=selected_programs,
            program_course_sets=pathway_program_course_sets,
            program_credit_sums=pathway_program_credit_sums,
            course_by_id=course_by_id,
            definitions=pathway_definitions,
        )
    )

    return {
        "version_id": version_id,
        "selected_program_ids": selected_program_ids,
        "include_core": include_core,
        "summary": {
            "top_level_total": total,
            "top_level_satisfied": satisfied,
            "top_level_remaining": max(0, total - satisfied),
            "completion_percent": (round((satisfied / total) * 100, 1) if total else 100.0),
        },
        "period_metadata": list_period_metadata(),
        "requirements": all_results,
        "core_rules": rule_nodes,
        "validation_items": validation_items,
    }


@app.get("/design/feasibility/{version_id}")
def design_feasibility(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    programs = db.scalars(select(AcademicProgram).where(AcademicProgram.version_id == version_id).order_by(AcademicProgram.name.asc())).all()
    majors = [p for p in programs if (p.program_type or "").upper() == "MAJOR"]
    minors = [p for p in programs if (p.program_type or "").upper() == "MINOR"]
    reqs = db.scalars(select(Requirement).where(Requirement.version_id == version_id).order_by(Requirement.sort_order.asc())).all()
    req_by_id = {r.id: r for r in reqs}
    program_by_id = {p.id: p for p in programs}
    node_code_map = build_program_designer_code_map(reqs, program_by_id)
    child_map: dict[Optional[str], list[Requirement]] = {}
    for r in reqs:
        child_map.setdefault(r.parent_requirement_id, []).append(r)
    fulfillments = db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_([r.id for r in reqs]))).all() if reqs else []
    links_by_req: dict[str, list[RequirementFulfillment]] = {}
    for f in fulfillments:
        links_by_req.setdefault(f.requirement_id, []).append(f)
    req_basket_links = (
        db.scalars(select(RequirementBasketLink).where(RequirementBasketLink.requirement_id.in_([r.id for r in reqs]))).all() if reqs else []
    )
    basket_ids = [x.basket_id for x in req_basket_links]
    basket_items = db.scalars(select(CourseBasketItem).where(CourseBasketItem.basket_id.in_(basket_ids))).all() if basket_ids else []
    basket_course_ids_by_basket: dict[str, list[str]] = {}
    for item in basket_items:
        basket_course_ids_by_basket.setdefault(item.basket_id, []).append(item.course_id)
    baskets_by_req: dict[str, list[RequirementBasketLink]] = {}
    for bl in req_basket_links:
        baskets_by_req.setdefault(bl.requirement_id, []).append(bl)
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    course_by_id = {c.id: c for c in courses}
    course_id_by_number = {normalize_course_number(c.course_number): c.id for c in courses}
    prereqs = db.scalars(select(CoursePrerequisite).where(CoursePrerequisite.course_id.in_([c.id for c in courses]))).all() if courses else []
    req_sub_rows = (
        db.scalars(select(RequirementSubstitution).where(RequirementSubstitution.requirement_id.in_([r.id for r in reqs]))).all() if reqs else []
    )
    sub_map: dict[str, dict[str, set[str]]] = {}
    for s in req_sub_rows:
        m = sub_map.setdefault(s.requirement_id, {})
        m.setdefault(s.primary_course_id, set()).add(s.substitute_course_id)
        if s.is_bidirectional:
            m.setdefault(s.substitute_course_id, set()).add(s.primary_course_id)
    bucket_rows = db.scalars(select(CourseBucketTag).where(CourseBucketTag.course_id.in_([c.id for c in courses]))).all() if courses else []
    rules = []
    rules_with_cfg: list[tuple[ValidationRule, dict]] = []
    for r in db.scalars(select(ValidationRule).where(ValidationRule.active.is_(True))).all():
        try:
            cfg = json.loads(r.config_json or "{}")
        except Exception:
            cfg = {}
        cfg = cfg if isinstance(cfg, dict) else {}
        if is_cadet_performance_rule(cfg):
            continue
        rules.append(r)
        rules_with_cfg.append((r, cfg))
    max_credits_per_semester = 21.0
    max_credits_per_summer_period = 9.0
    for rule in rules:
        try:
            cfg = json.loads(rule.config_json or "{}")
        except Exception:
            cfg = {}
        t = str(cfg.get("type") or "").upper()
        if t not in {"MAX_CREDITS_PER_SEMESTER", "SEMESTER_CREDIT_CAP"}:
            continue
        for key in ["max_credits_per_semester", "max_credits", "cap", "value"]:
            raw = cfg.get(key)
            if raw is None:
                continue
            try:
                cap = float(raw)
            except Exception:
                continue
            if cap > 0:
                max_credits_per_semester = min(max_credits_per_semester, cap)
        for key in ["max_credits_per_summer_period", "max_summer_credits", "summer_cap"]:
            raw = cfg.get(key)
            if raw is None:
                continue
            try:
                cap = float(raw)
            except Exception:
                continue
            if cap > 0:
                max_credits_per_summer_period = min(max_credits_per_summer_period, cap)

    req_name_map: dict[str, list[Requirement]] = {}
    for r in reqs:
        req_name_map.setdefault((r.name or "").strip().lower(), []).append(r)

    req_names_sorted = sorted(
        [(r.name or "", r.id) for r in reqs if (r.name or "").strip()],
        key=lambda x: len(x[0]),
        reverse=True,
    )

    def infer_node_code_from_message(msg: str) -> str:
        text_msg = str(msg or "").lower()
        for req_name, req_id in req_names_sorted:
            if req_name and req_name.lower() in text_msg:
                return node_code_map.get(req_id, "")
        return ""

    # Find active Core Rules keyed by target program.
    core_rules_by_program_id: dict[str, list[dict]] = {}
    core_rules_by_program_name: dict[str, list[dict]] = {}
    for rule in rules:
        try:
            cfg = json.loads(rule.config_json or "{}")
        except Exception:
            cfg = {}
        if str(cfg.get("type") or "").upper() not in {"MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"}:
            continue
        groups = cfg.get("required_core_groups") or []
        if cfg.get("program_id"):
            core_rules_by_program_id.setdefault(cfg["program_id"], []).extend(groups)
        if str(cfg.get("program_name") or "").strip():
            core_rules_by_program_name.setdefault(str(cfg["program_name"]).strip().lower(), []).extend(groups)

    def evaluate_requirement(req_id: str) -> dict:
        req = req_by_id[req_id]
        children = child_map.get(req_id, [])
        child_results = [evaluate_requirement(c.id) for c in children]
        links = links_by_req.get(req_id, [])
        issues: list[str] = []
        own_issues: list[str] = []
        constraints = []
        mandatory_courses = set()
        min_credit_lb = 0.0
        units = []
        for link in links:
            c = course_by_id.get(link.course_id)
            credit = float(c.credit_hours if c else 0.0)
            units.append(
                {
                    "label": c.course_number if c else link.course_id,
                    "type": "course",
                    "course_ids": [link.course_id],
                    "mandatory_courses": {link.course_id},
                    "min_credit_lb": credit,
                }
            )
        for bl in baskets_by_req.get(req_id, []):
            basket_course_ids = list(dict.fromkeys(basket_course_ids_by_basket.get(bl.basket_id, [])))
            needed = max(1, int(bl.min_count or 1))
            basket_courses = [course_by_id[cid] for cid in basket_course_ids if cid in course_by_id]
            credit_candidates = sorted([float(c.credit_hours or 0.0) for c in basket_courses])
            basket_min_credit = sum(credit_candidates[:needed]) if len(credit_candidates) >= needed else 0.0
            units.append(
                {
                    "label": f"Basket ({needed} of {len(basket_course_ids)})",
                    "type": "basket",
                    "course_ids": basket_course_ids,
                    "mandatory_courses": set(),
                    "min_credit_lb": basket_min_credit,
                    "min_count": needed,
                    "available_count": len(basket_course_ids),
                }
            )
        for child_req, child_eval in zip(children, child_results):
            units.append(
                {
                    "label": child_req.name,
                    "type": "requirement",
                    "course_ids": list(child_eval["all_courses"]),
                    "mandatory_courses": set(child_eval["mandatory_courses"]),
                    "min_credit_lb": float(child_eval["min_credit_lb"]),
                }
            )
            issues.extend(child_eval["issues"])
            constraints.extend(child_eval["constraints"])
            mandatory_courses |= set(child_eval["always_mandatory_courses"])
        logic = (req.logic_type or "ALL_REQUIRED").upper()
        available = len(units)
        if logic == "OPTION_SLOT":
            needed = max(1, int(req.option_slot_capacity or req.pick_n or 1))
            if available < needed:
                own_issues.append(f"Requires {needed} options but only {available} defined.")
            else:
                choice_units = sorted(units, key=lambda u: float(u["min_credit_lb"]))[:needed]
                min_credit_lb += sum(float(u["min_credit_lb"]) for u in choice_units)
            constraints.append(f"{req.name}: option slot requires {needed} choice(s) of {available}.")
        elif logic in {"PICK_N", "ANY_N"}:
            needed = max(1, int(req.pick_n or 1))
            if available < needed:
                own_issues.append(f"Requires {needed} options but only {available} defined.")
            else:
                choice_units = sorted(units, key=lambda u: float(u["min_credit_lb"]))[:needed]
                min_credit_lb += sum(float(u["min_credit_lb"]) for u in choice_units)
            constraints.append(f"{req.name}: choose {needed} of {available}.")
        elif logic in {"ANY_ONE", "ONE_OF"}:
            if available < 1:
                own_issues.append("Requires one option but none defined.")
            else:
                min_credit_lb += min(float(u["min_credit_lb"]) for u in units)
            constraints.append(f"{req.name}: choose 1 of {available}.")
        else:
            if not units:
                own_issues.append("No linked courses/subrequirements defined.")
            for u in units:
                if u["type"] == "basket":
                    if int(u.get("available_count") or 0) < int(u.get("min_count") or 1):
                        own_issues.append(
                            f"{u['label']} has fewer available courses than required ({u.get('available_count', 0)}<{u.get('min_count', 1)})."
                        )
                mandatory_courses |= set(u["mandatory_courses"])
                min_credit_lb += float(u["min_credit_lb"])
                if u["type"] == "requirement":
                    mandatory_courses |= set(u["mandatory_courses"])
        issues.extend(own_issues)
        all_courses = set()
        for u in units:
            all_courses |= set(u["course_ids"])
        # always_mandatory_courses excludes optional parent-choice units
        always_mandatory_courses = set(mandatory_courses if logic == "ALL_REQUIRED" else set())
        child_consistency_nodes = [c["consistency_node"] for c in child_results]
        node_status = "INCONSISTENT" if own_issues or any(x.get("status") == "INCONSISTENT" for x in child_consistency_nodes) else "CONSISTENT"
        consistency_node = {
            "requirement_id": req.id,
            "node_code": node_code_map.get(req.id, ""),
            "name": req.name,
            "status": node_status,
            "message": " | ".join(own_issues) if own_issues else "",
            "children": child_consistency_nodes,
        }
        return {
            "issues": issues,
            "constraints": constraints,
            "mandatory_courses": mandatory_courses,
            "always_mandatory_courses": always_mandatory_courses,
            "all_courses": all_courses,
            "min_credit_lb": min_credit_lb,
            "consistency_node": consistency_node,
        }

    def gather_program_requirement_courses(program_id: str) -> tuple[set[str], float]:
        top_program_reqs = [r for r in reqs if r.program_id == program_id and r.parent_requirement_id is None]
        out_courses: set[str] = set()
        out_credits = 0.0
        for tr in top_program_reqs:
            ev = evaluate_requirement(tr.id)
            out_courses |= set(ev["all_courses"])
            out_credits += float(ev["min_credit_lb"])
        return out_courses, out_credits

    def evaluate_combo(combo_programs: list[AcademicProgram], kind: str, label: str) -> dict:
        issues: list[str] = []
        constraints: list[str] = []
        selected_ids = {p.id for p in combo_programs}
        top_reqs = []
        for r in reqs:
            if r.parent_requirement_id is not None:
                continue
            if r.program_id is None and (r.category or "").upper() == "CORE":
                top_reqs.append(r)
            elif r.program_id in selected_ids:
                top_reqs.append(r)
        mandatory = set()
        all_courses = set()
        min_credit_lb = 0.0
        consistency_roots = []
        for tr in top_reqs:
            e = evaluate_requirement(tr.id)
            issues.extend(e["issues"])
            constraints.extend(e["constraints"])
            mandatory |= set(e["always_mandatory_courses"])
            all_courses |= set(e["all_courses"])
            min_credit_lb += float(e["min_credit_lb"])
            consistency_roots.append(e["consistency_node"])

        # Program/major pathway validation checks.
        pathway_definitions = get_pathway_definitions(rules_with_cfg)
        pathway_program_course_sets: dict[str, set[str]] = {}
        pathway_program_credit_sums: dict[str, float] = {}
        for p in combo_programs:
            pcs, pcredits = gather_program_requirement_courses(p.id)
            pathway_program_course_sets[p.id] = set(pcs)
            pathway_program_credit_sums[p.id] = float(pcredits)
        pathway_validation_items = build_pathway_validation_items(
            rule_rows=rules_with_cfg,
            programs=combo_programs,
            program_course_sets=pathway_program_course_sets,
            program_credit_sums=pathway_program_credit_sums,
            course_by_id=course_by_id,
            definitions=pathway_definitions,
        )

        # Requirement timing windows and clashes.
        timing_by_course: dict[str, list[tuple[Optional[int], Optional[int], Optional[int], str]]] = {}
        scoped_req_ids = {r.id for r in reqs if (r.program_id is None and (r.category or "").upper() == "CORE") or r.program_id in selected_ids}
        for rf in fulfillments:
            if rf.requirement_id not in scoped_req_ids:
                continue
            if rf.required_semester is None and rf.required_semester_min is None and rf.required_semester_max is None:
                continue
            req = req_by_id.get(rf.requirement_id)
            src = req.name if req else "Requirement"
            timing_by_course.setdefault(rf.course_id, []).append((rf.required_semester, rf.required_semester_min, rf.required_semester_max, src))
        for cid, windows in timing_by_course.items():
            for i in range(len(windows)):
                for j in range(i + 1, len(windows)):
                    a = windows[i]
                    b = windows[j]
                    if not timing_constraints_overlap(a[0], a[1], a[2], b[0], b[1], b[2]):
                        cnum = course_by_id.get(cid).course_number if course_by_id.get(cid) else cid
                        issues.append(f"{cnum}: timing clash between '{a[3]}' and '{b[3]}'.")

        # Core Rules compatibility (with core timing) and constraints.
        core_req_ids = {r.id for r in reqs if (r.program_id is None and (r.category or "").upper() == "CORE")}
        core_timing_by_course: dict[str, list[tuple[Optional[int], Optional[int], Optional[int]]]] = {}
        for rf in fulfillments:
            if rf.requirement_id not in core_req_ids:
                continue
            if rf.required_semester is None and rf.required_semester_min is None and rf.required_semester_max is None:
                continue
            core_timing_by_course.setdefault(rf.course_id, []).append((rf.required_semester, rf.required_semester_min, rf.required_semester_max))
        for p in combo_programs:
            groups = []
            groups.extend(core_rules_by_program_id.get(p.id, []))
            groups.extend(core_rules_by_program_name.get((p.name or "").strip().lower(), []))
            for idx, g in enumerate(groups):
                g = g or {}
                group_name = str(g.get("name") or f"Core Rule {idx + 1}").strip()
                min_count = max(1, int(g.get("min_count") or 1))
                rs = g.get("required_semester")
                rs_min = g.get("required_semester_min")
                rs_max = g.get("required_semester_max")
                try:
                    rs = int(rs) if rs is not None else None
                except Exception:
                    rs = None
                try:
                    rs_min = int(rs_min) if rs_min is not None else None
                except Exception:
                    rs_min = None
                try:
                    rs_max = int(rs_max) if rs_max is not None else None
                except Exception:
                    rs_max = None
                nums = [normalize_course_number(str(x)) for x in (g.get("course_numbers") or []) if str(x).strip()]
                cids = [course_id_by_number[n] for n in nums if n in course_id_by_number]
                if not cids:
                    issues.append(f"{p.name} - {group_name}: no resolvable courses.")
                    continue
                viable = 0
                for cid in cids:
                    core_windows = core_timing_by_course.get(cid, [])
                    if rs is None and rs_min is None and rs_max is None:
                        viable += 1
                        continue
                    if not core_windows:
                        viable += 1
                        continue
                    if any(timing_constraints_overlap(rs, rs_min, rs_max, cw[0], cw[1], cw[2]) for cw in core_windows):
                        viable += 1
                if viable < min_count:
                    issues.append(f"{p.name} - {group_name}: timing leaves only {viable} viable choices, needs {min_count}.")
                sem_parts = []
                if rs is not None:
                    sem_parts.append(period_short_label(rs))
                if rs_min is not None:
                    sem_parts.append(f">={period_short_label(rs_min)}")
                if rs_max is not None:
                    sem_parts.append(f"<={period_short_label(rs_max)}")
                if sem_parts:
                    constraints.append(f"{p.name} - {group_name}: must satisfy {' ,'.join(sem_parts)}.")

        # Cross-program Core Rules timing clashes (major-major / major-minor / minor-minor).
        core_rule_windows_by_course: dict[str, list[tuple[Optional[int], Optional[int], Optional[int], str]]] = {}
        for p in combo_programs:
            groups = []
            groups.extend(core_rules_by_program_id.get(p.id, []))
            groups.extend(core_rules_by_program_name.get((p.name or "").strip().lower(), []))
            for idx, g in enumerate(groups):
                g = g or {}
                rs = g.get("required_semester")
                rs_min = g.get("required_semester_min")
                rs_max = g.get("required_semester_max")
                try:
                    rs = int(rs) if rs is not None else None
                except Exception:
                    rs = None
                try:
                    rs_min = int(rs_min) if rs_min is not None else None
                except Exception:
                    rs_min = None
                try:
                    rs_max = int(rs_max) if rs_max is not None else None
                except Exception:
                    rs_max = None
                if rs is None and rs_min is None and rs_max is None:
                    continue
                group_name = str(g.get("name") or f"Core Rule {idx + 1}").strip()
                source = f"{p.name} - {group_name}"
                nums = [normalize_course_number(str(x)) for x in (g.get("course_numbers") or []) if str(x).strip()]
                for num in nums:
                    cid = course_id_by_number.get(num)
                    if not cid:
                        continue
                    core_rule_windows_by_course.setdefault(cid, []).append((rs, rs_min, rs_max, source))
        for cid, windows in core_rule_windows_by_course.items():
            for i in range(len(windows)):
                for j in range(i + 1, len(windows)):
                    a = windows[i]
                    b = windows[j]
                    if not timing_constraints_overlap(a[0], a[1], a[2], b[0], b[1], b[2]):
                        cnum = course_by_id.get(cid).course_number if course_by_id.get(cid) else cid
                        issues.append(f"{cnum}: Core Rules timing clash between '{a[3]}' and '{b[3]}'.")

        # Dependency feasibility for mandatory courses.
        windows: dict[str, tuple[int, int]] = {}
        for cid in mandatory:
            lo, hi = 0, MAX_PLAN_PERIOD
            for rs, rs_min, rs_max, _ in timing_by_course.get(cid, []):
                if rs is not None:
                    lo = max(lo, rs)
                    hi = min(hi, rs)
                if rs_min is not None:
                    lo = max(lo, rs_min)
                if rs_max is not None:
                    hi = min(hi, rs_max)
            if lo > hi:
                cnum = course_by_id.get(cid).course_number if course_by_id.get(cid) else cid
                issues.append(
                    f"{cnum}: no feasible period window ({period_short_label(lo)}>{period_short_label(hi)})."
                )
            windows[cid] = (lo, hi)
        for p in prereqs:
            if p.course_id not in mandatory or p.required_course_id not in mandatory:
                continue
            a_lo, a_hi = windows.get(p.required_course_id, (0, MAX_PLAN_PERIOD))
            b_lo, b_hi = windows.get(p.course_id, (0, MAX_PLAN_PERIOD))
            rel = (p.relationship_type or "PREREQUISITE").upper()
            feasible = False
            for sa in range(a_lo, a_hi + 1):
                for sb in range(b_lo, b_hi + 1):
                    if rel == "COREQUISITE":
                        if sa <= sb:
                            feasible = True
                            break
                    else:
                        if sa < sb:
                            feasible = True
                            break
                if feasible:
                    break
            if not feasible:
                req_num = course_by_id.get(p.required_course_id).course_number if course_by_id.get(p.required_course_id) else p.required_course_id
                course_num = course_by_id.get(p.course_id).course_number if course_by_id.get(p.course_id) else p.course_id
                issues.append(f"{course_num}: dependency on {req_num} infeasible within timing windows.")

        # Credit cap checks.
        total_credit_capacity = (max_credits_per_semester * float(len(ACADEMIC_PERIODS))) + (
            max_credits_per_summer_period * float(len(SUMMER_PERIODS))
        )
        residency_hours_rule, residency_hours_cfg = find_active_rule(
            rules_with_cfg,
            rule_type="RESIDENCY_MIN_HOURS",
            names=["Residency minimum in-residence hours", "COI residency requirements"],
        )
        residency_min_hours = DEFAULT_RESIDENCY_MIN_HOURS
        try:
            residency_min_hours = float(
                residency_hours_cfg.get("min_hours", residency_hours_cfg.get("minimum_in_residence_hours", residency_min_hours))
            )
        except Exception:
            residency_min_hours = DEFAULT_RESIDENCY_MIN_HOURS
        if min_credit_lb < residency_min_hours:
            constraints.append(
                f"Residency requires >= {residency_min_hours:.0f} in-residence credit hours; "
                f"defined minimum is {min_credit_lb:.1f}, so additional electives/requirements are needed."
            )
        if min_credit_lb > total_credit_capacity:
            issues.append(
                f"Minimum required credits {min_credit_lb:.1f} exceeds total period capacity {total_credit_capacity:.1f} "
                f"({max_credits_per_semester:.1f} x {len(ACADEMIC_PERIODS)} academic, "
                f"{max_credits_per_summer_period:.1f} x {len(SUMMER_PERIODS)} summer)."
            )
        avg = min_credit_lb / float(len(ALL_PLAN_PERIODS)) if min_credit_lb > 0 else 0.0
        if avg > max_credits_per_semester * 0.9:
            constraints.append(
                f"Average load {avg:.1f}/period is near academic cap {max_credits_per_semester:.1f}; limited schedule flexibility."
            )

        # De-duplicate message lists while preserving order.
        seen = set()
        issues_dedup = []
        for i in issues:
            if i in seen:
                continue
            seen.add(i)
            issues_dedup.append(i)
        seen = set()
        constraints_dedup = []
        for c in constraints:
            if c in seen:
                continue
            seen.add(c)
            constraints_dedup.append(c)
        validation_items = []
        validation_items.append(
            {
                "rule_code": str(residency_hours_rule.rule_code or "").strip() if residency_hours_rule else "",
                "rule_name": residency_hours_rule.name if residency_hours_rule else "Residency minimum in-residence hours",
                "status": ("PASS" if min_credit_lb >= residency_min_hours else "FAIL"),
                "message": f"Defined minimum in-residence hours {min_credit_lb:.1f}; threshold {residency_min_hours:.0f}.",
            }
        )
        validation_items.extend(
            abet_validation_items_for_courses(
                rules_with_cfg,
                programs=combo_programs,
                course_ids=set(mandatory),
                bucket_rows=bucket_rows,
                course_by_id=course_by_id,
            )
        )
        validation_items.extend(pathway_validation_items)
        if not consistency_roots:
            consistency_roots = [
                {
                    "requirement_id": "none",
                    "node_code": "",
                    "name": "Program Rules",
                    "status": "CONSISTENT",
                    "message": "",
                    "children": [],
                }
            ]
        consistency_items = []
        def flatten_consistency(nodes: list[dict], depth: int = 0):
            for n in nodes or []:
                consistency_items.append(
                    {
                        "node_code": n.get("node_code", ""),
                        "name": n.get("name", ""),
                        "status": n.get("status", "CONSISTENT"),
                        "message": n.get("message", ""),
                        "depth": depth,
                    }
                )
                flatten_consistency(n.get("children") or [], depth + 1)
        flatten_consistency(consistency_roots)
        consistency_fail_count = sum(1 for x in consistency_items if x.get("status") == "INCONSISTENT")
        consistency_pass_count = sum(1 for x in consistency_items if x.get("status") == "CONSISTENT")
        consistency_status = "INCONSISTENT" if consistency_fail_count > 0 else "CONSISTENT"
        status = "FAIL" if issues_dedup else "PASS"
        gate_rule, _ = find_active_rule(
            rules_with_cfg,
            rule_type="PROGRAM_FEASIBILITY_GATE",
            names=["Program feasibility gate"],
        )
        validation_items.append(
            {
                "rule_code": str(gate_rule.rule_code or "").strip() if gate_rule else "",
                "rule_name": gate_rule.name if gate_rule else "Program feasibility gate",
                "status": status,
                "message": f"{len(issues_dedup)} consistency conflicts detected.",
            }
        )
        validation_fail_count = sum(1 for x in validation_items if str(x.get("status") or "").upper() == "FAIL")
        validation_pass_count = sum(1 for x in validation_items if str(x.get("status") or "").upper() == "PASS")
        status = "FAIL" if (validation_fail_count > 0 or consistency_fail_count > 0) else "PASS"
        return {
            "kind": kind,
            "label": label,
            "program_ids": [p.id for p in combo_programs],
            "program_names": [p.name for p in combo_programs],
            "status": status,
            "consistency_status": consistency_status,
            "overall_status": f"{status} / {consistency_status}",
            "issue_count": len(issues_dedup),
            "issues": issues_dedup,
            "program_design_consistency_items": consistency_items,
            "program_design_consistency_tree": consistency_roots,
            "validation_pass_count": validation_pass_count,
            "validation_fail_count": validation_fail_count,
            "consistency_pass_count": consistency_pass_count,
            "consistency_fail_count": consistency_fail_count,
            "mandatory_course_count": len(mandatory),
            "min_required_credits": round(min_credit_lb, 1),
            "max_credits_per_semester": max_credits_per_semester,
            "max_credits_per_summer_period": max_credits_per_summer_period,
            "residency_hours_minimum": residency_min_hours,
            "validation_items": validation_items,
        }

    rows = []
    for m in majors:
        rows.append(evaluate_combo([m], "MAJOR", f"Major - {m.name}"))
    for n in minors:
        rows.append(evaluate_combo([n], "MINOR", f"Minor - {n.name}"))
    for a, b in itertools.combinations(majors, 2):
        rows.append(evaluate_combo([a, b], "DOUBLE_MAJOR", f"Double Major - {a.name} + {b.name}"))
    for m in majors:
        for n in minors:
            rows.append(evaluate_combo([m, n], "MAJOR_MINOR", f"Major/Minor - {m.name} + {n.name}"))

    return {
        "version_id": version_id,
        "period_metadata": list_period_metadata(),
        "row_count": len(rows),
        "summary": {
            "pass": sum(1 for r in rows if r["status"] == "PASS"),
            "warning": 0,
            "fail": sum(1 for r in rows if r["status"] == "FAIL"),
        },
        "rows": rows,
    }


@app.get("/design/impact/{version_id}")
def impact(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    hours = {i: 0.0 for i in ALL_PLAN_PERIODS}
    for item in db.scalars(select(PlanItem).where(PlanItem.version_id == version_id)).all():
        c = db.get(Course, item.course_id)
        if c:
            hours[item.semester_index] += c.credit_hours
    return {
        "credit_hours_by_semester": hours,
        "credit_hours_by_period": hours,
        "period_metadata": list_period_metadata(),
    }


@app.get("/design/impact-analysis/{version_id}")
def impact_analysis(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    canvas_items = db.scalars(select(PlanItem).where(PlanItem.version_id == version_id)).all()
    course_by_id: dict[str, Course] = {}
    semester_hours = {i: 0.0 for i in ALL_PLAN_PERIODS}
    canvas_course_ids: set[str] = set()

    for item in canvas_items:
        course = db.get(Course, item.course_id)
        if not course:
            continue
        course_by_id[course.id] = course
        canvas_course_ids.add(course.id)
        semester_hours[item.semester_index] += course.credit_hours

    affected_programs = set()
    for rf in db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.course_id.in_(canvas_course_ids))).all() if canvas_course_ids else []:
        req = db.get(Requirement, rf.requirement_id)
        if req and req.program_id:
            program = db.get(AcademicProgram, req.program_id)
            if program:
                affected_programs.add(program.name)

    prereq_warnings = []
    for pre in db.scalars(select(CoursePrerequisite)).all():
        course = db.get(Course, pre.course_id)
        required = db.get(Course, pre.required_course_id)
        if not course or not required:
            continue
        if course.version_id != version_id or required.version_id != version_id:
            continue
        if (
            course.designated_semester is not None
            and required.designated_semester is not None
            and required.designated_semester >= course.designated_semester
        ):
            prereq_warnings.append(f"{required.course_number} should be before {course.course_number}")

    return {
        "credit_hours_by_semester": semester_hours,
        "credit_hours_by_period": semester_hours,
        "period_metadata": list_period_metadata(),
        "affected_programs": sorted(affected_programs),
        "prerequisite_warnings": prereq_warnings,
        "estimated_resource_delta": {
            "sections": len(canvas_course_ids),
            "instructors_needed_floor": len(canvas_course_ids),
        },
    }


@app.get("/design/validation/{version_id}")
def validate(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    findings = []
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    rules = []
    rules_with_cfg = []
    for r in db.scalars(select(ValidationRule).where(ValidationRule.active == True)).all():  # noqa: E712
        try:
            cfg = json.loads(r.config_json or "{}")
        except Exception:
            cfg = {}
        cfg = cfg if isinstance(cfg, dict) else {}
        if is_cadet_performance_rule(cfg) or is_definitional_rule(cfg):
            continue
        rules.append(r)
        rules_with_cfg.append((r, cfg))
    rule_lookup = {r.name: r for r in rules}
    plan_items = db.scalars(select(PlanItem).where(PlanItem.version_id == version_id)).all()
    planned_course_ids = {item.course_id for item in plan_items}
    planned_course_semesters: dict[str, set[int]] = {}
    for item in plan_items:
        planned_course_semesters.setdefault(item.course_id, set()).add(item.semester_index)
    course_id_by_number = {normalize_course_number(c.course_number): c.id for c in courses}
    course_number_by_id = {c.id: c.course_number for c in courses}
    reqs = db.scalars(select(Requirement).where(Requirement.version_id == version_id)).all()
    req_by_id = {r.id: r for r in reqs}
    req_by_name: dict[str, list[Requirement]] = {}
    req_children: dict[Optional[str], list[Requirement]] = {}
    for r in reqs:
        req_children.setdefault(r.parent_requirement_id, []).append(r)
        req_by_name.setdefault(str(r.name or "").strip().lower(), []).append(r)
    req_links = db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_([r.id for r in reqs]))).all() if reqs else []
    links_by_req: dict[str, list[RequirementFulfillment]] = {}
    for link in req_links:
        links_by_req.setdefault(link.requirement_id, []).append(link)

    def collect_requirement_course_ids(requirement_id: str) -> set[str]:
        out: set[str] = set()
        stack = [requirement_id]
        seen: set[str] = set()
        while stack:
            rid = stack.pop()
            if rid in seen:
                continue
            seen.add(rid)
            for link in links_by_req.get(rid, []):
                out.add(link.course_id)
            for child in req_children.get(rid, []):
                stack.append(child.id)
        return out

    # Minimum section size baseline
    min_rule = rule_lookup.get("Minimum section size >= 6")
    min_value = 6
    if min_rule and min_rule.config_json:
        try:
            min_value = int(json.loads(min_rule.config_json).get("minimum", 6))
        except Exception:
            min_value = 6
    for c in courses:
        if c.min_section_size < min_value:
            findings.append(
                {
                    "severity": (min_rule.severity if min_rule else "WARNING"),
                    "tier": (min_rule.tier if min_rule else 1),
                    "rule_code": str(min_rule.rule_code or "").strip() if min_rule else "",
                    "rule": (min_rule.name if min_rule else "Minimum section size >= 6"),
                    "message": f"{c.course_number} min section size {c.min_section_size} is below {min_value}",
                }
            )

    # Semester credit bound checks
    max_credits = 24
    max_rule = rule_lookup.get("Semester credit upper bound")
    if max_rule and max_rule.config_json:
        try:
            max_credits = float(json.loads(max_rule.config_json).get("max_credits", 24))
        except Exception:
            max_credits = 24
    hours = {i: 0.0 for i in ALL_PLAN_PERIODS}
    for item in plan_items:
        c = db.get(Course, item.course_id)
        if c:
            hours[item.semester_index] += c.credit_hours
    for sem, total in hours.items():
        if total > max_credits:
            findings.append(
                {
                    "severity": (max_rule.severity if max_rule else "WARNING"),
                    "tier": (max_rule.tier if max_rule else 1),
                    "rule_code": str(max_rule.rule_code or "").strip() if max_rule else "",
                    "rule": (max_rule.name if max_rule else "Semester credit upper bound"),
                    "message": f"{period_label(sem)} has {total} credit hours (max {max_credits}).",
                }
            )

    # Residency checks (editable validation rules).
    residency_hours_rule, residency_hours_cfg = find_active_rule(
        rules_with_cfg,
        rule_type="RESIDENCY_MIN_HOURS",
        names=["Residency minimum in-residence hours", "COI residency requirements"],
    )
    residency_sem_rule, residency_sem_cfg = find_active_rule(
        rules_with_cfg,
        rule_type="RESIDENCY_MIN_ACADEMIC_SEMESTERS",
        names=["Residency minimum academic semesters", "COI residency requirements"],
    )
    residency_min_hours = DEFAULT_RESIDENCY_MIN_HOURS
    residency_min_academic = DEFAULT_RESIDENCY_MIN_ACADEMIC_SEMESTERS
    try:
        residency_min_hours = float(
            residency_hours_cfg.get("min_hours", residency_hours_cfg.get("minimum_in_residence_hours", residency_min_hours))
        )
    except Exception:
        residency_min_hours = DEFAULT_RESIDENCY_MIN_HOURS
    try:
        residency_min_academic = int(
            residency_sem_cfg.get("min_semesters", residency_sem_cfg.get("minimum_academic_semesters", residency_min_academic))
        )
    except Exception:
        residency_min_academic = DEFAULT_RESIDENCY_MIN_ACADEMIC_SEMESTERS
    total_planned_hours = sum(hours.values())
    academic_periods_with_load = sum(1 for p in ACADEMIC_PERIODS if float(hours.get(p, 0.0)) > 0.0)
    if total_planned_hours < residency_min_hours:
        findings.append(
            {
                "severity": (residency_hours_rule.severity if residency_hours_rule else "WARNING"),
                "tier": (residency_hours_rule.tier if residency_hours_rule else 1),
                "rule_code": str(residency_hours_rule.rule_code or "").strip() if residency_hours_rule else "",
                "rule": (residency_hours_rule.name if residency_hours_rule else "Residency minimum in-residence hours"),
                "message": (
                    f"Planned in-residence credit hours {total_planned_hours:.1f} are below required "
                    f"{residency_min_hours:.0f}."
                ),
            }
        )
    if academic_periods_with_load < residency_min_academic:
        findings.append(
            {
                "severity": (residency_sem_rule.severity if residency_sem_rule else "WARNING"),
                "tier": (residency_sem_rule.tier if residency_sem_rule else 1),
                "rule_code": str(residency_sem_rule.rule_code or "").strip() if residency_sem_rule else "",
                "rule": (residency_sem_rule.name if residency_sem_rule else "Residency minimum academic semesters"),
                "message": (
                    f"Planned academic semesters with load {academic_periods_with_load} are below required "
                    f"{residency_min_academic}."
                ),
            }
        )

    # Prerequisite sequencing checks based on designated semester when available
    pre_rule = rule_lookup.get("Prerequisite ordering")
    for pre in db.scalars(select(CoursePrerequisite)).all():
        course = db.get(Course, pre.course_id)
        required = db.get(Course, pre.required_course_id)
        if not course or not required:
            continue
        if course.version_id != version_id or required.version_id != version_id:
            continue
        if (
            course.designated_semester is not None
            and required.designated_semester is not None
            and required.designated_semester >= course.designated_semester
        ):
            findings.append(
                {
                    "severity": (pre_rule.severity if pre_rule else "FAIL"),
                    "tier": (pre_rule.tier if pre_rule else 1),
                    "rule_code": str(pre_rule.rule_code or "").strip() if pre_rule else "",
                    "rule": (pre_rule.name if pre_rule else "Prerequisite ordering"),
                    "message": f"{required.course_number} should occur before {course.course_number}.",
                }
            )

    # Resource constraints: classroom capacity, instructor load, qualification
    sections = db.scalars(select(Section).where(Section.version_id == version_id)).all()
    instructor_load: dict[str, int] = {}
    room_usage: dict[tuple[str, str], int] = {}
    cap_rule = rule_lookup.get("Classroom capacity constraints")
    load_rule = rule_lookup.get("Instructor load limits")
    qual_rule = rule_lookup.get("Instructor qualification constraints")

    for sec in sections:
        course = db.get(Course, sec.course_id)
        if not course:
            continue

        if sec.classroom_id:
            room = db.get(Classroom, sec.classroom_id)
            if not room:
                findings.append(
                    {
                            "severity": (cap_rule.severity if cap_rule else "WARNING"),
                            "tier": (cap_rule.tier if cap_rule else 3),
                            "rule_code": str(cap_rule.rule_code or "").strip() if cap_rule else "",
                            "rule": (cap_rule.name if cap_rule else "Classroom capacity constraints"),
                            "message": f"Section {sec.id} references missing classroom {sec.classroom_id}.",
                        }
                    )
            else:
                if room.capacity < sec.max_enrollment:
                    findings.append(
                        {
                            "severity": (cap_rule.severity if cap_rule else "WARNING"),
                            "tier": (cap_rule.tier if cap_rule else 3),
                            "rule_code": str(cap_rule.rule_code or "").strip() if cap_rule else "",
                            "rule": (cap_rule.name if cap_rule else "Classroom capacity constraints"),
                            "message": f"{room.building} {room.room_number} capacity {room.capacity} < section max {sec.max_enrollment}.",
                        }
                    )
                key = (sec.semester_label, sec.classroom_id)
                room_usage[key] = room_usage.get(key, 0) + 1

        if sec.instructor_id:
            instructor = db.get(Instructor, sec.instructor_id)
            if not instructor:
                findings.append(
                    {
                            "severity": (load_rule.severity if load_rule else "WARNING"),
                            "tier": (load_rule.tier if load_rule else 3),
                            "rule_code": str(load_rule.rule_code or "").strip() if load_rule else "",
                            "rule": (load_rule.name if load_rule else "Instructor load limits"),
                            "message": f"Section {sec.id} references missing instructor {sec.instructor_id}.",
                        }
                    )
            else:
                instructor_load[instructor.id] = instructor_load.get(instructor.id, 0) + 1
                qualification = db.scalar(
                    select(InstructorQualification).where(
                        InstructorQualification.instructor_id == instructor.id, InstructorQualification.course_id == sec.course_id
                    )
                )
                if not qualification:
                    findings.append(
                        {
                            "severity": (qual_rule.severity if qual_rule else "WARNING"),
                            "tier": (qual_rule.tier if qual_rule else 3),
                            "rule_code": str(qual_rule.rule_code or "").strip() if qual_rule else "",
                            "rule": (qual_rule.name if qual_rule else "Instructor qualification constraints"),
                            "message": f"Instructor {instructor.name} is not qualified for {course.course_number}.",
                        }
                    )

    for instructor_id, count in instructor_load.items():
        instructor = db.get(Instructor, instructor_id)
        if instructor and instructor.max_sections_per_semester is not None and count > instructor.max_sections_per_semester:
            findings.append(
                {
                    "severity": (load_rule.severity if load_rule else "WARNING"),
                    "tier": (load_rule.tier if load_rule else 3),
                    "rule_code": str(load_rule.rule_code or "").strip() if load_rule else "",
                    "rule": (load_rule.name if load_rule else "Instructor load limits"),
                    "message": f"Instructor {instructor.name} assigned {count} sections (max {instructor.max_sections_per_semester}).",
                }
            )

    for (semester_label, classroom_id), count in room_usage.items():
        if count > 1:
            room = db.get(Classroom, classroom_id)
            room_name = f"{room.building} {room.room_number}" if room else classroom_id
            findings.append(
                {
                    "severity": (cap_rule.severity if cap_rule else "WARNING"),
                    "tier": (cap_rule.tier if cap_rule else 3),
                    "rule_code": str(cap_rule.rule_code or "").strip() if cap_rule else "",
                    "rule": (cap_rule.name if cap_rule else "Classroom capacity constraints"),
                    "message": f"Room {room_name} has {count} sections in {semester_label} (possible conflict).",
                }
            )

    # Program/Major core pathway rules are enforced in Program Design Rules checks
    # (checklist + feasibility) and intentionally excluded from this Validation Rules
    # engine to avoid duplicate findings.
    for rule in rules:
        cfg = {}
        try:
            cfg = json.loads(rule.config_json or "{}")
        except Exception:
            cfg = {}
        rule_type = str(cfg.get("type") or "").upper()
        if rule_type not in {"MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"}:
            continue
        continue

        program_id = cfg.get("program_id")
        program_name = str(cfg.get("program_name") or "").strip()
        target_program = None
        if program_id:
            target_program = db.get(AcademicProgram, program_id)
        elif program_name:
            target_program = db.scalar(
                select(AcademicProgram).where(
                    AcademicProgram.version_id == version_id,
                    AcademicProgram.name == program_name,
                )
            )
        if not target_program:
            findings.append(
                {
                    "severity": rule.severity,
                    "tier": rule.tier,
                    "rule_code": str(rule.rule_code or "").strip(),
                    "rule": "major_pathway_rule_scope",
                    "message": f"Pathway rule '{rule.name}' target program not found.",
                }
            )
            continue

        groups = cfg.get("required_core_groups") or []
        if not isinstance(groups, list) or not groups:
            findings.append(
                {
                    "severity": rule.severity,
                    "tier": rule.tier,
                    "rule_code": str(rule.rule_code or "").strip(),
                    "rule": "major_pathway_rule_config",
                    "message": f"Pathway rule '{rule.name}' has no required_core_groups.",
                }
            )
            continue

        for idx, g in enumerate(groups):
            g = g or {}
            group_name = str(g.get("name") or f"Group {idx + 1}").strip()
            min_count = int(g.get("min_count") or 1)
            min_count = max(1, min_count)
            required_semester = g.get("required_semester")
            required_semester_min = g.get("required_semester_min")
            required_semester_max = g.get("required_semester_max")
            try:
                required_semester = int(required_semester) if required_semester is not None else None
            except Exception:
                required_semester = None
            try:
                required_semester_min = int(required_semester_min) if required_semester_min is not None else None
            except Exception:
                required_semester_min = None
            try:
                required_semester_max = int(required_semester_max) if required_semester_max is not None else None
            except Exception:
                required_semester_max = None
            if required_semester_min is not None and required_semester_max is not None and required_semester_min > required_semester_max:
                findings.append(
                    {
                        "severity": rule.severity,
                        "tier": rule.tier,
                        "rule_code": str(rule.rule_code or "").strip(),
                        "rule": "major_pathway_group_config",
                        "message": f"{target_program.name}: '{group_name}' has invalid semester range.",
                    }
                )
                continue
            if required_semester is not None and (required_semester_min is not None or required_semester_max is not None):
                findings.append(
                    {
                        "severity": rule.severity,
                        "tier": rule.tier,
                        "rule_code": str(rule.rule_code or "").strip(),
                        "rule": "major_pathway_group_config",
                        "message": f"{target_program.name}: '{group_name}' mixes fixed semester with range constraints.",
                    }
                )
                continue
            group_course_ids: set[str] = set()

            for num in g.get("course_numbers") or []:
                cid = course_id_by_number.get(normalize_course_number(str(num)))
                if cid:
                    group_course_ids.add(cid)
            for req_id in g.get("requirement_ids") or []:
                if req_id in req_by_id:
                    group_course_ids |= collect_requirement_course_ids(req_id)
            for req_name in g.get("requirement_names") or []:
                for req in req_by_name.get(str(req_name).strip().lower(), []):
                    group_course_ids |= collect_requirement_course_ids(req.id)

            if not group_course_ids:
                findings.append(
                    {
                        "severity": rule.severity,
                        "tier": rule.tier,
                        "rule_code": str(rule.rule_code or "").strip(),
                        "rule": "major_pathway_group_config",
                        "message": f"{target_program.name}: '{group_name}' has no resolvable courses.",
                    }
                )
                continue

            matched_ids = sorted([cid for cid in group_course_ids if cid in planned_course_ids])
            if required_semester is not None or required_semester_min is not None or required_semester_max is not None:
                def semester_ok(sem: int) -> bool:
                    if required_semester is not None and sem != required_semester:
                        return False
                    if required_semester_min is not None and sem < required_semester_min:
                        return False
                    if required_semester_max is not None and sem > required_semester_max:
                        return False
                    return True
                matched_ids = sorted([cid for cid in matched_ids if any(semester_ok(s) for s in planned_course_semesters.get(cid, set()))])
            if len(matched_ids) < min_count:
                matched_nums = [course_number_by_id.get(cid) for cid in matched_ids if course_number_by_id.get(cid)]
                sem_parts = []
                if required_semester is not None:
                    sem_parts.append(period_short_label(required_semester))
                if required_semester_min is not None:
                    sem_parts.append(f">={period_short_label(required_semester_min)}")
                if required_semester_max is not None:
                    sem_parts.append(f"<={period_short_label(required_semester_max)}")
                findings.append(
                    {
                        "severity": rule.severity,
                        "tier": rule.tier,
                        "rule_code": str(rule.rule_code or "").strip(),
                        "rule": "major_pathway_core",
                        "message": (
                            f"{target_program.name}: '{group_name}' needs {min_count}, has {len(matched_ids)}"
                            + (f" [{', '.join(sem_parts)}]" if sem_parts else "")
                            + (f" ({', '.join(matched_nums)})" if matched_nums else "")
                        ),
                    }
                )

    status = "PASS"
    if any(f["severity"] == "FAIL" for f in findings):
        status = "FAIL"
    elif findings:
        status = "WARNING"
    return {"status": status, "findings": findings, "period_metadata": list_period_metadata()}


@app.get("/design/validation-dashboard/{version_id}")
def validation_dashboard(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    result = validate(version_id, db, _)
    findings = result["findings"]
    by_severity = {"FAIL": 0, "WARNING": 0, "PASS": 0}
    by_tier = {1: 0, 2: 0, 3: 0}
    for f in findings:
        sev = f.get("severity", "WARNING")
        by_severity[sev] = by_severity.get(sev, 0) + 1
        tier = int(f.get("tier", 1))
        by_tier[tier] = by_tier.get(tier, 0) + 1
    total = len(findings)
    active_design_count = len(active_design_rules_with_config(db))
    pass_count = max(0, active_design_count - total)
    by_severity["PASS"] = pass_count
    return {
        "status": result["status"],
        "counts_by_severity": by_severity,
        "counts_by_tier": by_tier,
        "findings": findings,
        "period_metadata": result.get("period_metadata", list_period_metadata()),
    }


@app.get("/design/requirements/gap-analysis/{cadet_id}")
def cadet_gap_analysis(cadet_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    cadet = db.get(Cadet, cadet_id)
    if not cadet:
        raise HTTPException(status_code=404, detail="Cadet not found")

    completed = {
        r.course_id
        for r in db.scalars(select(CadetRecord).where(CadetRecord.cadet_id == cadet_id, CadetRecord.is_completed == True)).all()  # noqa: E712
    }
    unmet = []
    met = []
    if cadet.major_program_id:
        reqs = db.scalars(select(Requirement).where(Requirement.program_id == cadet.major_program_id).order_by(Requirement.sort_order.asc())).all()
        for req in reqs:
            options = db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id == req.id)).all()
            if not options:
                unmet.append({"requirement": req.name, "reason": "No fulfillment mapping defined"})
                continue
            option_course_ids = {o.course_id for o in options}
            if option_course_ids & completed:
                met.append(req.name)
            else:
                unmet.append({"requirement": req.name, "required_course_ids": sorted(option_course_ids)})

    return {"cadet_id": cadet_id, "met_requirements": met, "unmet_requirements": unmet}


@app.get("/design/validation-rules")
def list_validation_rules(db: Session = Depends(get_db), _: User = Depends(current_user)):
    ensure_validation_rule_codes(db)
    return [
        serialize(r)
        for r in db.scalars(select(ValidationRule).order_by(ValidationRule.rule_code.asc(), ValidationRule.tier.asc(), ValidationRule.name.asc())).all()
    ]


@app.post("/design/validation-rules")
def create_validation_rule(payload: ValidationRuleIn, db: Session = Depends(get_db), user: User = Depends(require_design)):
    validate_core_pathway_rule_config(payload.config or {}, db)
    requested_code = str(payload.rule_code or "").strip().upper()
    if requested_code and db.scalar(select(ValidationRule).where(ValidationRule.rule_code == requested_code)):
        raise HTTPException(status_code=400, detail="Validation rule code already exists")
    rule = ValidationRule(
        rule_code=(requested_code or next_validation_rule_code(db)),
        name=payload.name,
        tier=payload.tier,
        severity=payload.severity,
        active=payload.active,
        config_json=json.dumps(payload.config),
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    write_audit(db, user, "CREATE", "ValidationRule", rule.id, str(payload.model_dump()))
    return serialize(rule)


@app.put("/design/validation-rules/{rule_id}")
def update_validation_rule(rule_id: str, payload: ValidationRuleUpdateIn, db: Session = Depends(get_db), user: User = Depends(require_design)):
    rule = db.get(ValidationRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Validation rule not found")
    data = payload.model_dump(exclude_unset=True)
    if "rule_code" in data:
        requested_code = str(data["rule_code"] or "").strip().upper()
        if requested_code:
            existing = db.scalar(select(ValidationRule).where(ValidationRule.rule_code == requested_code))
            if existing and existing.id != rule.id:
                raise HTTPException(status_code=400, detail="Validation rule code already exists")
            rule.rule_code = requested_code
        else:
            rule.rule_code = next_validation_rule_code(db)
    if "name" in data:
        rule.name = data["name"]
    if "tier" in data:
        rule.tier = data["tier"]
    if "severity" in data:
        rule.severity = data["severity"]
    if "active" in data:
        rule.active = data["active"]
    if "config" in data:
        validate_core_pathway_rule_config(data["config"] or {}, db)
        rule.config_json = json.dumps(data["config"] or {})
    db.commit()
    db.refresh(rule)
    write_audit(db, user, "UPDATE", "ValidationRule", rule.id, str(data))
    return serialize(rule)


@app.post("/design/validation-rules/{rule_id}/toggle")
def toggle_validation_rule(rule_id: str, active: bool, db: Session = Depends(get_db), user: User = Depends(require_design)):
    rule = db.get(ValidationRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Validation rule not found")
    rule.active = active
    db.commit()
    write_audit(db, user, "TOGGLE", "ValidationRule", rule.id, str(active))
    return {"id": rule.id, "active": rule.active}


@app.delete("/design/validation-rules/{rule_id}")
def delete_validation_rule(rule_id: str, db: Session = Depends(get_db), user: User = Depends(require_design)):
    rule = db.get(ValidationRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Validation rule not found")
    db.delete(rule)
    db.commit()
    write_audit(db, user, "DELETE", "ValidationRule", rule_id)
    return {"status": "deleted"}


@app.post("/design/transition/cohort")
def cohort_assign(class_year: int, version_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    obj = CohortAssignment(class_year=class_year, version_id=version_id)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return serialize(obj)


@app.post("/design/transition/equivalency")
def add_equivalency(from_version_id: str, to_version_id: str, from_course_id: str, to_course_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    obj = CourseEquivalency(from_version_id=from_version_id, to_version_id=to_version_id, from_course_id=from_course_id, to_course_id=to_course_id)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return serialize(obj)


@app.get("/design/transition/cohort")
def list_cohorts(db: Session = Depends(get_db), _: User = Depends(current_user)):
    return [serialize(c) for c in db.scalars(select(CohortAssignment).order_by(CohortAssignment.class_year.asc())).all()]


@app.get("/design/transition/equivalency")
def list_equivalencies(from_version_id: Optional[str] = None, to_version_id: Optional[str] = None, db: Session = Depends(get_db), _: User = Depends(current_user)):
    stmt = select(CourseEquivalency)
    if from_version_id:
        stmt = stmt.where(CourseEquivalency.from_version_id == from_version_id)
    if to_version_id:
        stmt = stmt.where(CourseEquivalency.to_version_id == to_version_id)
    return [serialize(e) for e in db.scalars(stmt).all()]


@app.get("/design/transition/impact")
def transition_impact(from_version_id: str, to_version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    from_courses = db.scalars(select(Course).where(Course.version_id == from_version_id)).all()
    mapped = {
        eq.from_course_id
        for eq in db.scalars(
            select(CourseEquivalency).where(CourseEquivalency.from_version_id == from_version_id, CourseEquivalency.to_version_id == to_version_id)
        ).all()
    }
    unmapped = [c.course_number for c in from_courses if c.id not in mapped]
    return {"unmapped_course_count": len(unmapped), "unmapped_courses": sorted(unmapped)}


@app.post("/design/comments")
def create_comment(payload: DesignCommentIn, db: Session = Depends(get_db), user: User = Depends(require_design)):
    obj = DesignComment(**payload.model_dump(), created_by=user.id)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    write_audit(db, user, "COMMENT", payload.entity_type, payload.entity_id, payload.comment)
    return serialize(obj)


@app.get("/design/comments/{version_id}")
def list_comments(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    return [serialize(c) for c in db.scalars(select(DesignComment).where(DesignComment.version_id == version_id).order_by(DesignComment.created_at.desc())).all()]


@app.post("/design/change-requests")
def create_change_request(payload: ChangeRequestIn, db: Session = Depends(get_db), user: User = Depends(require_design)):
    obj = ChangeRequest(version_id=payload.version_id, title=payload.title, description=payload.description, proposed_by=user.id)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    write_audit(db, user, "PROPOSE", "ChangeRequest", obj.id, payload.title)
    return serialize(obj)


@app.post("/design/change-requests/{request_id}/review")
def review_change_request(request_id: str, approve: bool, db: Session = Depends(get_db), user: User = Depends(require_design)):
    obj = db.get(ChangeRequest, request_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Change request not found")
    obj.status = "APPROVED" if approve else "REJECTED"
    obj.reviewed_by = user.id
    db.commit()
    write_audit(db, user, "REVIEW", "ChangeRequest", obj.id, obj.status)
    return {"status": obj.status}


@app.get("/design/change-requests/{version_id}")
def list_change_requests(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    return [serialize(c) for c in db.scalars(select(ChangeRequest).where(ChangeRequest.version_id == version_id).order_by(ChangeRequest.created_at.desc())).all()]


@app.post("/import/csv/courses")
def import_course_csv(file: UploadFile = File(...), db: Session = Depends(get_db), _: User = Depends(require_design)):
    data = file.file.read().decode("utf-8-sig")
    inserted = 0
    errors = []
    for i, row in enumerate(csv.DictReader(io.StringIO(data)), start=2):
        try:
            obj = CourseIn(**row)
            db.add(Course(**obj.model_dump()))
            inserted += 1
        except Exception as exc:
            errors.append({"line": i, "error": str(exc)})
    db.commit()
    return {"inserted": inserted, "errors": errors}


@app.post("/import/csv/{entity_name}")
def import_csv_entity(entity_name: str, file: UploadFile = File(...), db: Session = Depends(get_db), _: User = Depends(require_design)):
    schema_map = {
        "courses": (CourseIn, Course),
        "programs": (ProgramIn, AcademicProgram),
        "requirements": (RequirementIn, Requirement),
        "instructors": (InstructorIn, Instructor),
        "classrooms": (ClassroomIn, Classroom),
        "sections": (SectionIn, Section),
        "cadets": (CadetIn, Cadet),
        "records": (CadetRecordIn, CadetRecord),
        "prerequisites": (PrerequisiteIn, CoursePrerequisite),
        "substitutions": (SubstitutionIn, CourseSubstitution),
    }
    if entity_name not in schema_map:
        raise HTTPException(status_code=400, detail=f"Unsupported entity '{entity_name}'")

    schema_cls, model_cls = schema_map[entity_name]
    data = file.file.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(data))
    inserted = 0
    errors = []

    for i, row in enumerate(reader, start=2):
        try:
            parsed = schema_cls(**row)
            payload = parsed.model_dump()
            if entity_name == "substitutions":
                payload["conditions_json"] = json.dumps(payload.pop("conditions", {}) or {})
            obj = model_cls(**payload)
            db.add(obj)
            inserted += 1
        except Exception as exc:
            errors.append({"line": i, "error": str(exc), "row": row})

    db.commit()
    return {"entity": entity_name, "inserted": inserted, "errors": errors}


@app.post("/import/csv/{entity_name}/validate")
def validate_csv_entity(entity_name: str, file: UploadFile = File(...), db: Session = Depends(get_db), _: User = Depends(require_design)):
    schema_map = {
        "courses": (CourseIn, Course),
        "programs": (ProgramIn, AcademicProgram),
        "requirements": (RequirementIn, Requirement),
        "instructors": (InstructorIn, Instructor),
        "classrooms": (ClassroomIn, Classroom),
        "sections": (SectionIn, Section),
        "cadets": (CadetIn, Cadet),
        "records": (CadetRecordIn, CadetRecord),
        "prerequisites": (PrerequisiteIn, CoursePrerequisite),
        "substitutions": (SubstitutionIn, CourseSubstitution),
    }
    if entity_name not in schema_map:
        raise HTTPException(status_code=400, detail=f"Unsupported entity '{entity_name}'")

    schema_cls, _model_cls = schema_map[entity_name]
    data = file.file.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(data))
    valid_rows = 0
    errors = []
    sample_valid = []

    for i, row in enumerate(reader, start=2):
        try:
            parsed = schema_cls(**row)
            valid_rows += 1
            if len(sample_valid) < 10:
                sample_valid.append(parsed.model_dump())
        except Exception as exc:
            errors.append({"line": i, "error": str(exc), "row": row})

    return {
        "entity": entity_name,
        "valid_rows": valid_rows,
        "error_count": len(errors),
        "errors": errors,
        "sample_valid_rows": sample_valid,
    }


@app.post("/import/coi/text")
def parse_coi_text(file: UploadFile = File(...), _: User = Depends(require_design)):
    raise HTTPException(status_code=410, detail="COI auto-parsing is disabled. Use manual Program Designer + dataset import workflows.")


@app.post("/import/coi/analyze")
def analyze_coi(
    options: CoiLoadOptions = Depends(),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _: User = Depends(require_design),
):
    raise HTTPException(status_code=410, detail="COI auto-parsing is disabled. Use manual Program Designer + dataset import workflows.")


@app.post("/import/coi/review/start")
def start_coi_review(
    options: CoiLoadOptions = Depends(),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    raise HTTPException(status_code=410, detail="COI auto-review parsing is disabled. Use manual Program Designer + dataset import workflows.")


@app.get("/import/coi/review/{session_id}")
def get_coi_review_session(session_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    raise HTTPException(status_code=410, detail="COI auto-review parsing is disabled. Use manual Program Designer + dataset import workflows.")


@app.post("/import/coi/review/{session_id}/decide")
def decide_coi_review(
    session_id: str,
    payload: CoiReviewDecisionsIn,
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    raise HTTPException(status_code=410, detail="COI auto-review parsing is disabled. Use manual Program Designer + dataset import workflows.")


@app.post("/import/coi/review/{session_id}/commit")
def commit_coi_review(
    session_id: str,
    options: CoiReviewCommitOptions,
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    raise HTTPException(status_code=410, detail="COI auto-review parsing is disabled. Use manual Program Designer + dataset import workflows.")


@app.post("/import/coi/load-baseline")
def load_coi_baseline(
    options: CoiLoadOptions = Depends(),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    raise HTTPException(status_code=410, detail="COI baseline auto-parsing is disabled. Use manual Program Designer + dataset import workflows.")


@app.get("/audit")
def audit_feed(limit: int = Query(200, ge=1, le=1000), db: Session = Depends(get_db), _: User = Depends(require_design)):
    return [serialize(a) for a in db.scalars(select(AuditLog).order_by(AuditLog.created_at.desc()).limit(limit)).all()]
