from __future__ import annotations

import csv
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


class ValidationRule(Base):
    __tablename__ = "validation_rules"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String, unique=True)
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
    required_semester: Optional[int] = Field(default=None, ge=1, le=8)
    required_semester_min: Optional[int] = Field(default=None, ge=1, le=8)
    required_semester_max: Optional[int] = Field(default=None, ge=1, le=8)


class RequirementSubstitutionIn(BaseModel):
    requirement_id: str
    primary_course_id: str
    substitute_course_id: str
    is_bidirectional: bool = False


class MoveIn(BaseModel):
    plan_item_id: str
    target_semester: int = Field(ge=1, le=8)
    target_position: int = Field(ge=0)


class CanvasItemUpdateIn(BaseModel):
    semester_index: Optional[int] = Field(default=None, ge=1, le=8)
    category: Optional[str] = None
    major_mode: Optional[str] = None
    major_program_id: Optional[str] = None
    track_name: Optional[str] = None
    aspect: Optional[str] = None


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


class ValidationRuleIn(BaseModel):
    name: str
    tier: int = Field(ge=1, le=3)
    severity: str = "WARNING"
    active: bool = True
    config: dict = Field(default_factory=dict)


class ValidationRuleUpdateIn(BaseModel):
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
    for s in range(1, 9):
        if semester_constraint_allows(s, a_required, a_min, a_max) and semester_constraint_allows(s, b_required, b_min, b_max):
            return True
    return False


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


def ensure_runtime_migrations() -> None:
    with engine.begin() as conn:
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
        plan_cols = conn.execute(text("PRAGMA table_info(plan_items)")).fetchall()
        plan_col_names = {c[1] for c in plan_cols}
        if "aspect" not in plan_col_names:
            conn.execute(text("ALTER TABLE plan_items ADD COLUMN aspect TEXT DEFAULT 'CORE'"))
        if "major_program_id" not in plan_col_names:
            conn.execute(text("ALTER TABLE plan_items ADD COLUMN major_program_id TEXT"))
        if "track_name" not in plan_col_names:
            conn.execute(text("ALTER TABLE plan_items ADD COLUMN track_name TEXT"))
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
        prog_cols = conn.execute(text("PRAGMA table_info(academic_programs)")).fetchall()
        prog_col_names = {c[1] for c in prog_cols}
        if "division" not in prog_col_names:
            conn.execute(text("ALTER TABLE academic_programs ADD COLUMN division TEXT"))


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
        design_user = db.scalar(select(User).where(User.username == "design_admin"))
        seed_demo_data(db, actor_user_id=design_user.id if design_user else None)
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

    def build(parent: Optional[str]):
        nodes = []
        for req in by_parent.get(parent, []):
            program = db.get(AcademicProgram, req.program_id) if req.program_id else None
            nodes.append(
                {
                    "id": req.id,
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
                    "courses": links_by_req.get(req.id, []),
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
    return {
        "general": serialize(course),
        "scheduling": {
            "designated_semester": course.designated_semester,
            "credit_hours": course.credit_hours,
            "min_section_size": course.min_section_size,
        },
        "prerequisites": [serialize(p) for p in prereqs],
        "requirements": linked_requirements,
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


@app.get("/design/canvas/{version_id}")
def canvas(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    out = {str(i): [] for i in range(1, 9)}
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
    semester_index: int = Query(..., ge=1, le=8),
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
    courses = db.scalars(select(Course).where(Course.version_id == version_id)).all()
    course_num_by_id = {c.id: c.course_number for c in courses}
    course_id_by_number = {normalize_course_number(c.course_number): c.id for c in courses}
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
            for child in req_children.get(cur, []):
                stack.append(child.id)
        collect_cache[req_id] = set(found)
        return found

    def evaluate(req_id: str) -> dict:
        req = req_by_id[req_id]
        children = child_map.get(req_id, [])
        child_results = [evaluate(c.id) for c in children]
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
                    parts.append(f"Semester {required_semester}")
                if required_semester_min is not None:
                    parts.append(f"Semester {required_semester_min}+")
                if required_semester_max is not None:
                    parts.append(f"Semester <= {required_semester_max}")
                window_text = ", ".join(parts) if parts else "required semester window"
                fixed_semester_violations.append(
                    f"{' / '.join(option_nums)} must satisfy: {window_text}"
                )
        matched_count = len(matched_course_ids)
        child_pass_count = sum(1 for c in child_results if c["is_satisfied"])
        unit_required = len(link_course_ids) + len(child_results)
        unit_satisfied = matched_count + child_pass_count

        logic = (req.logic_type or "ALL_REQUIRED").upper()
        if logic in {"PICK_N", "ANY_N"}:
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
                    sem_bits.append(f"S{required_semester}")
                if required_semester_min is not None:
                    sem_bits.append(f">=S{required_semester_min}")
                if required_semester_max is not None:
                    sem_bits.append(f"<=S{required_semester_max}")
                sem_suffix = f" [{', '.join(sem_bits)}]" if sem_bits else ""
                if matched_ids_without_timing and not matched_ids:
                    violations.append(f"Choice is present but violates timing rule{sem_suffix}.")
            child_satisfied = len(matched_ids) >= min_count and not violations
            children.append(
                {
                    "requirement_id": f"core-rule-group:{rule.id}:{idx}",
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
        program_prefix = "Minor" if (target_program.program_type or "").upper() == "MINOR" else "Major"
        rule_nodes.append(
            {
                "requirement_id": f"core-rule:{rule.id}",
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

    all_results = [*results, *rule_nodes]
    total = len(all_results)
    satisfied = sum(1 for r in all_results if r["is_satisfied"])
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
        "requirements": all_results,
        "core_rules": rule_nodes,
    }


@app.get("/design/feasibility/{version_id}")
def design_feasibility(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    programs = db.scalars(select(AcademicProgram).where(AcademicProgram.version_id == version_id).order_by(AcademicProgram.name.asc())).all()
    majors = [p for p in programs if (p.program_type or "").upper() == "MAJOR"]
    minors = [p for p in programs if (p.program_type or "").upper() == "MINOR"]
    reqs = db.scalars(select(Requirement).where(Requirement.version_id == version_id).order_by(Requirement.sort_order.asc())).all()
    req_by_id = {r.id: r for r in reqs}
    child_map: dict[Optional[str], list[Requirement]] = {}
    for r in reqs:
        child_map.setdefault(r.parent_requirement_id, []).append(r)
    fulfillments = db.scalars(select(RequirementFulfillment).where(RequirementFulfillment.requirement_id.in_([r.id for r in reqs]))).all() if reqs else []
    links_by_req: dict[str, list[RequirementFulfillment]] = {}
    for f in fulfillments:
        links_by_req.setdefault(f.requirement_id, []).append(f)
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
    rules = db.scalars(select(ValidationRule).where(ValidationRule.active.is_(True))).all()
    max_credits_per_semester = 21.0
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

    req_name_map: dict[str, list[Requirement]] = {}
    for r in reqs:
        req_name_map.setdefault((r.name or "").strip().lower(), []).append(r)

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
        issues = []
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
        if logic in {"PICK_N", "ANY_N"}:
            needed = max(1, int(req.pick_n or 1))
            if available < needed:
                issues.append(f"{req.name}: requires {needed} options but only {available} defined.")
            else:
                choice_units = sorted(units, key=lambda u: float(u["min_credit_lb"]))[:needed]
                min_credit_lb += sum(float(u["min_credit_lb"]) for u in choice_units)
            constraints.append(f"{req.name}: choose {needed} of {available}.")
        elif logic in {"ANY_ONE", "ONE_OF"}:
            if available < 1:
                issues.append(f"{req.name}: requires one option but none defined.")
            else:
                min_credit_lb += min(float(u["min_credit_lb"]) for u in units)
            constraints.append(f"{req.name}: choose 1 of {available}.")
        else:
            if not units:
                issues.append(f"{req.name}: no linked courses/subrequirements defined.")
            for u in units:
                mandatory_courses |= set(u["mandatory_courses"])
                min_credit_lb += float(u["min_credit_lb"])
                if u["type"] == "requirement":
                    mandatory_courses |= set(u["mandatory_courses"])
        all_courses = set()
        for u in units:
            all_courses |= set(u["course_ids"])
        # always_mandatory_courses excludes optional parent-choice units
        always_mandatory_courses = set(mandatory_courses if logic == "ALL_REQUIRED" else set())
        return {
            "issues": issues,
            "constraints": constraints,
            "mandatory_courses": mandatory_courses,
            "always_mandatory_courses": always_mandatory_courses,
            "all_courses": all_courses,
            "min_credit_lb": min_credit_lb,
        }

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
        for tr in top_reqs:
            e = evaluate_requirement(tr.id)
            issues.extend(e["issues"])
            constraints.extend(e["constraints"])
            mandatory |= set(e["always_mandatory_courses"])
            all_courses |= set(e["all_courses"])
            min_credit_lb += float(e["min_credit_lb"])

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
                    sem_parts.append(f"S{rs}")
                if rs_min is not None:
                    sem_parts.append(f">=S{rs_min}")
                if rs_max is not None:
                    sem_parts.append(f"<=S{rs_max}")
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
            lo, hi = 1, 8
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
                issues.append(f"{cnum}: no feasible semester window ({lo}>{hi}).")
            windows[cid] = (lo, hi)
        for p in prereqs:
            if p.course_id not in mandatory or p.required_course_id not in mandatory:
                continue
            a_lo, a_hi = windows.get(p.required_course_id, (1, 8))
            b_lo, b_hi = windows.get(p.course_id, (1, 8))
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
        if min_credit_lb > max_credits_per_semester * 8.0:
            issues.append(
                f"Minimum required credits {min_credit_lb:.1f} exceeds {max_credits_per_semester:.1f} x 8 semester capacity."
            )
        avg = min_credit_lb / 8.0 if min_credit_lb > 0 else 0.0
        if avg > max_credits_per_semester * 0.9:
            constraints.append(
                f"Average load {avg:.1f}/semester is near max cap {max_credits_per_semester:.1f}; limited schedule flexibility."
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
        status = "PASS"
        if issues_dedup:
            status = "FAIL"
        elif constraints_dedup:
            status = "WARNING"
        return {
            "kind": kind,
            "label": label,
            "program_ids": [p.id for p in combo_programs],
            "program_names": [p.name for p in combo_programs],
            "status": status,
            "issue_count": len(issues_dedup),
            "constraint_count": len(constraints_dedup),
            "issues": issues_dedup,
            "constraints": constraints_dedup,
            "mandatory_course_count": len(mandatory),
            "min_required_credits": round(min_credit_lb, 1),
            "max_credits_per_semester": max_credits_per_semester,
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
        "row_count": len(rows),
        "summary": {
            "pass": sum(1 for r in rows if r["status"] == "PASS"),
            "warning": sum(1 for r in rows if r["status"] == "WARNING"),
            "fail": sum(1 for r in rows if r["status"] == "FAIL"),
        },
        "rows": rows,
    }


@app.get("/design/impact/{version_id}")
def impact(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    hours = {i: 0.0 for i in range(1, 9)}
    for item in db.scalars(select(PlanItem).where(PlanItem.version_id == version_id)).all():
        c = db.get(Course, item.course_id)
        if c:
            hours[item.semester_index] += c.credit_hours
    return {"credit_hours_by_semester": hours}


@app.get("/design/impact-analysis/{version_id}")
def impact_analysis(version_id: str, db: Session = Depends(get_db), _: User = Depends(current_user)):
    canvas_items = db.scalars(select(PlanItem).where(PlanItem.version_id == version_id)).all()
    course_by_id: dict[str, Course] = {}
    semester_hours = {i: 0.0 for i in range(1, 9)}
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
        if course.designated_semester and required.designated_semester and required.designated_semester >= course.designated_semester:
            prereq_warnings.append(f"{required.course_number} should be before {course.course_number}")

    return {
        "credit_hours_by_semester": semester_hours,
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
    rules = db.scalars(select(ValidationRule).where(ValidationRule.active == True)).all()  # noqa: E712

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

    # COI policy minimum section size baseline
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
                    "rule": "COI 2-3.9 minimum enrollment size",
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
    hours = {i: 0.0 for i in range(1, 9)}
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
                    "rule": "semester_credit_upper_bound",
                    "message": f"Semester {sem} has {total} credit hours (max {max_credits}).",
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
        if course.designated_semester and required.designated_semester and required.designated_semester >= course.designated_semester:
            findings.append(
                {
                    "severity": (pre_rule.severity if pre_rule else "FAIL"),
                    "tier": (pre_rule.tier if pre_rule else 1),
                    "rule": "prerequisite_ordering",
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
                        "rule": "classroom_missing",
                        "message": f"Section {sec.id} references missing classroom {sec.classroom_id}.",
                    }
                )
            else:
                if room.capacity < sec.max_enrollment:
                    findings.append(
                        {
                            "severity": (cap_rule.severity if cap_rule else "WARNING"),
                            "tier": (cap_rule.tier if cap_rule else 3),
                            "rule": "classroom_capacity",
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
                        "rule": "instructor_missing",
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
                            "rule": "instructor_unqualified",
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
                    "rule": "instructor_load",
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
                    "rule": "room_double_booked",
                    "message": f"Room {room_name} has {count} sections in {semester_label} (possible conflict).",
                }
            )

    # Program/Major pathway rules (within existing engine, config-driven)
    # Example config:
    # {
    #   "type": "MAJOR_PATHWAY_CORE",
    #   "program_name": "Aeronautical Engineering",
    #   "required_core_groups": [
    #     {"name": "Advanced STEM", "min_count": 1, "course_numbers": ["MATH 300", "PHYS 300"]},
    #     {"name": "Advanced Humanities", "min_count": 1, "requirement_names": ["Track - Advanced Liberal Arts: Any One"]}
    #   ]
    # }
    for rule in rules:
        cfg = {}
        try:
            cfg = json.loads(rule.config_json or "{}")
        except Exception:
            cfg = {}
        rule_type = str(cfg.get("type") or "").upper()
        if rule_type not in {"MAJOR_PATHWAY_CORE", "MAJOR_CORE_PATHWAY"}:
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
                    sem_parts.append(f"Semester {required_semester}")
                if required_semester_min is not None:
                    sem_parts.append(f"Semester {required_semester_min}+")
                if required_semester_max is not None:
                    sem_parts.append(f"Semester <= {required_semester_max}")
                findings.append(
                    {
                        "severity": rule.severity,
                        "tier": rule.tier,
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
    return {"status": status, "findings": findings}


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
    pass_count = max(0, len(db.scalars(select(ValidationRule).where(ValidationRule.active == True)).all()) - total)  # noqa: E712
    by_severity["PASS"] = pass_count
    return {"status": result["status"], "counts_by_severity": by_severity, "counts_by_tier": by_tier, "findings": findings}


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
    return [serialize(r) for r in db.scalars(select(ValidationRule).order_by(ValidationRule.tier.asc(), ValidationRule.name.asc())).all()]


@app.post("/design/validation-rules")
def create_validation_rule(payload: ValidationRuleIn, db: Session = Depends(get_db), user: User = Depends(require_design)):
    validate_core_pathway_rule_config(payload.config or {}, db)
    rule = ValidationRule(
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
    text = file.file.read().decode("utf-8", errors="ignore")
    policies = {
        "minimum_core_gpa_2_0": bool(re.search(r"minimum\\s+2\\.0\\s+core\\s+grade\\s+point\\s+average", text, re.IGNORECASE)),
        "minimum_cumulative_gpa_2_0": bool(re.search(r"2\\.0\\s+cumulative\\s+GPA", text, re.IGNORECASE)),
        "minimum_residency_hours_125": bool(re.search(r"at\\s+least\\s+125\\s+semester\\s+hours", text, re.IGNORECASE)),
    }
    course_numbers = sorted(set(re.findall(r"\\b[A-Z]{2,4}\\s?\\d{3}[A-Z]?\\b", text)))
    return {"policies": policies, "detected_course_numbers": course_numbers[:1500], "count": len(course_numbers)}


@app.post("/import/coi/analyze")
def analyze_coi(
    options: CoiLoadOptions = Depends(),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _: User = Depends(require_design),
):
    version = db.get(CurriculumVersion, options.version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")
    text = file.file.read().decode("utf-8", errors="ignore")
    parsed = parse_coi_courses(text, min_occurrences=options.min_course_number_occurrences)
    parsed_filtered = {k: v for k, v in parsed.items() if v.get("confidence", 0.0) >= options.min_confidence}
    existing = {c.course_number for c in db.scalars(select(Course).where(Course.version_id == options.version_id)).all()}
    to_create = [p for p in parsed_filtered.values() if p["course_number"] not in existing]
    to_update = [p for p in parsed_filtered.values() if p["course_number"] in existing]
    low_conf = [p for p in parsed.values() if p.get("confidence", 0.0) < options.min_confidence]
    by_source = {}
    for p in parsed.values():
        src = p.get("source", "unknown")
        by_source[src] = by_source.get(src, 0) + 1
    return {
        "version_id": options.version_id,
        "detected_courses": len(parsed),
        "above_confidence_threshold": len(parsed_filtered),
        "to_create": len(to_create),
        "to_update": len(to_update),
        "low_confidence_count": len(low_conf),
        "counts_by_source": by_source,
        "min_confidence": options.min_confidence,
        "sample_to_create": to_create[:30],
        "sample_low_confidence": low_conf[:30],
    }


@app.post("/import/coi/review/start")
def start_coi_review(
    options: CoiLoadOptions = Depends(),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    version = db.get(CurriculumVersion, options.version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")

    text = file.file.read().decode("utf-8", errors="ignore")
    parsed = parse_coi_courses(text, min_occurrences=options.min_course_number_occurrences)

    session = CoiImportSession(
        version_id=options.version_id,
        source_filename=file.filename,
        min_course_number_occurrences=options.min_course_number_occurrences,
        min_confidence=options.min_confidence,
        status="DRAFT",
        created_by=user.id,
    )
    db.add(session)
    db.commit()
    db.refresh(session)

    created = 0
    auto_approved = 0
    needs_review = 0
    for _, item in sorted(parsed.items()):
        include = item.get("confidence", 0.0) >= options.min_confidence
        db.add(
            CoiImportItem(
                session_id=session.id,
                course_number=item["course_number"],
                title=item["title"],
                occurrences=int(item.get("occurrences", 1)),
                confidence=float(item.get("confidence", 0.0)),
                source=item.get("source"),
                include=include,
            )
        )
        created += 1
        if include:
            auto_approved += 1
        else:
            needs_review += 1
    db.commit()

    write_audit(
        db,
        user,
        "COI_REVIEW_START",
        "CoiImportSession",
        session.id,
        json.dumps({"created": created, "auto_approved": auto_approved, "needs_review": needs_review}),
    )
    return {
        "session_id": session.id,
        "version_id": session.version_id,
        "created_items": created,
        "auto_approved": auto_approved,
        "needs_review": needs_review,
    }


@app.get("/import/coi/review/{session_id}")
def get_coi_review_session(session_id: str, db: Session = Depends(get_db), _: User = Depends(require_design)):
    session = db.get(CoiImportSession, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Review session not found")
    items = db.scalars(select(CoiImportItem).where(CoiImportItem.session_id == session_id).order_by(CoiImportItem.confidence.asc())).all()
    out = [serialize(i) for i in items]
    return {
        "session": serialize(session),
        "counts": {
            "total": len(out),
            "included": sum(1 for x in out if x["include"]),
            "excluded": sum(1 for x in out if not x["include"]),
        },
        "items": out,
    }


@app.post("/import/coi/review/{session_id}/decide")
def decide_coi_review(
    session_id: str,
    payload: CoiReviewDecisionsIn,
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    session = db.get(CoiImportSession, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Review session not found")
    updated = 0
    for d in payload.decisions:
        item = db.get(CoiImportItem, d.item_id)
        if not item or item.session_id != session_id:
            continue
        item.include = d.include
        if d.edited_title is not None:
            item.edited_title = d.edited_title.strip()[:240]
        updated += 1
    db.commit()
    write_audit(db, user, "COI_REVIEW_DECIDE", "CoiImportSession", session_id, json.dumps({"updated": updated}))
    return {"session_id": session_id, "updated": updated}


@app.post("/import/coi/review/{session_id}/commit")
def commit_coi_review(
    session_id: str,
    options: CoiReviewCommitOptions,
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    session = db.get(CoiImportSession, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Review session not found")
    if session.status == "COMMITTED":
        raise HTTPException(status_code=400, detail="Review session already committed")

    existing_by_number = {c.course_number: c for c in db.scalars(select(Course).where(Course.version_id == session.version_id)).all()}
    items = db.scalars(select(CoiImportItem).where(CoiImportItem.session_id == session_id, CoiImportItem.include == True)).all()  # noqa: E712
    inserted = 0
    updated = 0
    skipped = 0
    for item in items:
        title = (item.edited_title or item.title or "TBD Title").strip()
        code = item.course_number
        if code not in existing_by_number:
            db.add(
                Course(
                    version_id=session.version_id,
                    course_number=code,
                    title=title,
                    credit_hours=options.default_credit_hours,
                    min_section_size=6,
                )
            )
            inserted += 1
        else:
            if options.replace_existing:
                c = existing_by_number[code]
                c.title = title
                updated += 1
            else:
                skipped += 1
    session.status = "COMMITTED"
    db.commit()
    write_audit(
        db,
        user,
        "COI_REVIEW_COMMIT",
        "CoiImportSession",
        session_id,
        json.dumps({"inserted": inserted, "updated": updated, "skipped": skipped}),
    )
    return {"session_id": session_id, "inserted": inserted, "updated": updated, "skipped_existing": skipped}


@app.post("/import/coi/load-baseline")
def load_coi_baseline(
    options: CoiLoadOptions = Depends(),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_design),
):
    version = db.get(CurriculumVersion, options.version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")

    text = file.file.read().decode("utf-8", errors="ignore")
    parsed = parse_coi_courses(text, min_occurrences=options.min_course_number_occurrences)
    parsed = {k: v for k, v in parsed.items() if v.get("confidence", 0.0) >= options.min_confidence}
    existing_by_number = {c.course_number: c for c in db.scalars(select(Course).where(Course.version_id == options.version_id)).all()}

    inserted = 0
    updated = 0
    skipped = 0
    low_confidence = 0
    examples = []

    for code, data in sorted(parsed.items()):
        title = data["title"]
        occurrences = data["occurrences"]
        if data.get("confidence", 0.0) < options.min_confidence:
            low_confidence += 1

        if code not in existing_by_number:
            course = Course(
                version_id=options.version_id,
                course_number=code,
                title=title,
                credit_hours=options.default_credit_hours,
                min_section_size=6,
            )
            db.add(course)
            inserted += 1
            if len(examples) < 20:
                examples.append({"action": "insert", "course_number": code, "title": title, "occurrences": occurrences})
        else:
            if options.replace_existing:
                course = existing_by_number[code]
                course.title = title if title else course.title
                updated += 1
                if len(examples) < 20:
                    examples.append({"action": "update", "course_number": code, "title": title, "occurrences": occurrences})
            else:
                skipped += 1

    db.commit()
    write_audit(
        db,
        user,
        "IMPORT_COI_BASELINE",
        "CurriculumVersion",
        options.version_id,
        json.dumps(
            {
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
                "low_confidence": low_confidence,
                "min_occurrences": options.min_course_number_occurrences,
                "min_confidence": options.min_confidence,
            }
        ),
    )

    return {
        "version_id": options.version_id,
        "inserted": inserted,
        "updated": updated,
        "skipped_existing": skipped,
        "low_confidence_rows": low_confidence,
        "min_confidence": options.min_confidence,
        "sample_actions": examples,
    }


@app.get("/audit")
def audit_feed(limit: int = Query(200, ge=1, le=1000), db: Session = Depends(get_db), _: User = Depends(require_design)):
    return [serialize(a) for a in db.scalars(select(AuditLog).order_by(AuditLog.created_at.desc()).limit(limit)).all()]
