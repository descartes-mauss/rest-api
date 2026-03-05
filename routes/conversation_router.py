from typing import Any, Dict, List
from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.conversation import (
    ConversationCreateSchema,
    ConversationSchema,
    ConversationUpdateSchema,
)
from jwt_validator import validate_jwt
from repositories.conversation_repository import ConversationRepository
from services.conversation_service import ConversationService

conversation_router = APIRouter(prefix="/api/v2/conversations", tags=["conversations"])


def get_conversation_repository() -> ConversationRepository:
    """Deferred loading to allow test overrides."""
    from database import manager as db_manager

    return ConversationRepository(db_manager.db)


def get_conversation_service(
    repo: ConversationRepository = Depends(get_conversation_repository),
) -> ConversationService:
    return ConversationService(repo)


@conversation_router.get("/", response_model=List[ConversationSchema])
def list_conversations(
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: ConversationService = Depends(get_conversation_service),
) -> JSONResponse:
    """Return all conversations."""
    tenant_schema = authorization.get("orgId")
    conversations = service.get_all_conversations(tenant_schema)
    return JSONResponse(status_code=200, content=jsonable_encoder(conversations))


@conversation_router.get("/{conversation_id}", response_model=ConversationSchema)
def get_conversation(
    conversation_id: UUID,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: ConversationService = Depends(get_conversation_service),
) -> JSONResponse:
    """Return a single conversation by ID."""
    tenant_schema = authorization.get("orgId")
    conversation = service.get_conversation(tenant_schema, conversation_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(conversation))


@conversation_router.post("/", response_model=ConversationSchema, status_code=201)
def create_conversation(
    payload: ConversationCreateSchema,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: ConversationService = Depends(get_conversation_service),
) -> JSONResponse:
    """Create a new conversation."""
    tenant_schema = authorization.get("orgId")
    conversation = service.create_conversation(tenant_schema, payload)
    return JSONResponse(status_code=201, content=jsonable_encoder(conversation))


@conversation_router.put("/{conversation_id}", response_model=ConversationSchema)
def update_conversation(
    conversation_id: UUID,
    payload: ConversationUpdateSchema,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: ConversationService = Depends(get_conversation_service),
) -> JSONResponse:
    """Fully replace an existing conversation."""
    tenant_schema = authorization.get("orgId")
    conversation = service.update_conversation(tenant_schema, conversation_id, payload)
    return JSONResponse(status_code=200, content=jsonable_encoder(conversation))


@conversation_router.delete("/{conversation_id}", status_code=204)
def delete_conversation(
    conversation_id: UUID,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: ConversationService = Depends(get_conversation_service),
) -> JSONResponse:
    """Delete a conversation by ID."""
    tenant_schema = authorization.get("orgId")
    service.delete_conversation(tenant_schema, conversation_id)
    return JSONResponse(status_code=204, content=None)
