import datetime
from typing import Generator, List, Optional

import pytest
from fastapi.testclient import TestClient

from database.public_models.models import CIClient, ClientCompanyProfile
from database.tenant_models.models import Brand, BusinessCategory, CustomerSegment, ProductLine
from jwt_validator import validate_jwt
from main import app
from routes.company_router import get_company_service
from services.company_service import CompanyService


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    def override_validate_jwt() -> dict[str, str]:
        return {"orgId": "test_schema"}

    app.dependency_overrides[validate_jwt] = override_validate_jwt
    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides = {}


def make_ci_client(ci_id: int = 1) -> CIClient:
    return CIClient(
        id=ci_id,
        schema_name="test_schema",
        name="Test Client",
        org_id="test_schema",
        client_status="active",
    )


def make_company_profile(ci_id: int = 1) -> ClientCompanyProfile:
    now = datetime.datetime.now(datetime.UTC)
    return ClientCompanyProfile(
        id=10,
        client_id=ci_id,
        company_name="Acme Corp",
        brands_count=2,
        company_purpose="To innovate",
        company_purpose_implication=None,
        company_vision="Global leader",
        company_vision_implication=None,
        company_mission="Deliver value",
        company_mission_implication=None,
        company_personality=["bold", "innovative"],
        company_strategic_priorities=None,
        company_competitors=["Rival A", "Rival B"],
        created_at=now,
        updated_at=now,
    )


def make_brand(brand_id: int = 5, category_id: Optional[int] = 1) -> Brand:
    return Brand(
        id=brand_id,
        brand_name="Acme Brand",
        brand_description="A brand",
        brand_business_category_id=category_id,
    )


def make_business_category(category_id: int = 1) -> BusinessCategory:
    return BusinessCategory(
        id=category_id,
        business_category_name="Technology",
        business_category_description="Tech sector",
    )


def make_product_line(pl_id: int = 1, brand_id: int = 5) -> ProductLine:
    return ProductLine(
        id=pl_id,
        brand_id=brand_id,
        product_line_name="Alpha Line",
        product_line_user_benefit="Benefit A",
        product_line_value_proposition="Value A",
    )


def make_customer_segment(seg_id: int = 1) -> CustomerSegment:
    return CustomerSegment(
        id=seg_id,
        customer_segment_name="Enterprise",
        customer_segment_description="Large enterprises",
    )


def test_get_company_success(client: TestClient) -> None:
    ci_client = make_ci_client()
    profile = make_company_profile()
    brand = make_brand()
    category = make_business_category()
    product_line = make_product_line()
    segment = make_customer_segment()
    all_category = make_business_category(category_id=2)
    all_category.business_category_name = "Finance"
    all_category.business_category_description = "Finance sector"

    class FakeCompanyRepo:
        def get_ci_client(self, org_id: str) -> Optional[CIClient]:
            return ci_client

        def get_company_profile(self, ci_client_id: int) -> Optional[ClientCompanyProfile]:
            return profile

        def get_cs_client_image(self, org_id: str) -> Optional[str]:
            return "s3://bucket/image.png"

        def get_customer_segments(self, tenant_schema: str) -> List[CustomerSegment]:
            return [segment]

        def get_all_business_categories(self, tenant_schema: str) -> List[BusinessCategory]:
            return [category, all_category]

    class FakeBrandRepo:
        def get_brands(self, tenant_schema: str) -> List[Brand]:
            return [brand]

        def get_product_lines_by_brand_ids(
            self, tenant_schema: str, brand_ids: List[int]
        ) -> List[ProductLine]:
            return [product_line]

        def get_business_categories_by_ids(
            self, tenant_schema: str, category_ids: List[int]
        ) -> List[BusinessCategory]:
            return [category]

    app.dependency_overrides[get_company_service] = lambda: CompanyService(FakeCompanyRepo(), FakeBrandRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/companies")
    assert resp.status_code == 200
    data = resp.json()

    assert data["company_profile_image_uri"] == "s3://bucket/image.png"

    cp = data["company_profile"]
    assert cp is not None
    assert cp["company_name"] == "Acme Corp"
    assert cp["company_competitors"] == ["Rival A", "Rival B"]
    assert cp["company_personality"] == ["bold", "innovative"]

    assert len(data["brands"]) == 1
    b = data["brands"][0]
    assert b["brand_name"] == "Acme Brand"
    assert b["brand_business_category"]["business_category_name"] == "Technology"
    assert b["brand_business_category"]["name"] == "Technology"
    assert len(b["product_lines"]) == 1

    assert len(data["customer_segments"]) == 1
    seg = data["customer_segments"][0]
    assert seg["customer_segment_name"] == "Enterprise"
    assert seg["name"] == "Enterprise"
    assert seg["description"] == "Large enterprises"

    assert len(data["business_categories"]) == 2


def test_get_company_no_profile(client: TestClient) -> None:
    ci_client = make_ci_client()

    class FakeCompanyRepo:
        def get_ci_client(self, org_id: str) -> Optional[CIClient]:
            return ci_client

        def get_company_profile(self, ci_client_id: int) -> Optional[ClientCompanyProfile]:
            return None

        def get_cs_client_image(self, org_id: str) -> Optional[str]:
            return None

        def get_customer_segments(self, tenant_schema: str) -> List[CustomerSegment]:
            return []

        def get_all_business_categories(self, tenant_schema: str) -> List[BusinessCategory]:
            return []

    class FakeBrandRepo:
        def get_brands(self, tenant_schema: str) -> List[Brand]:
            return []

        def get_product_lines_by_brand_ids(
            self, tenant_schema: str, brand_ids: List[int]
        ) -> List[ProductLine]:
            return []

        def get_business_categories_by_ids(
            self, tenant_schema: str, category_ids: List[int]
        ) -> List[BusinessCategory]:
            return []

    app.dependency_overrides[get_company_service] = lambda: CompanyService(FakeCompanyRepo(), FakeBrandRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/companies")
    assert resp.status_code == 200
    data = resp.json()

    assert data["company_profile"] is None
    assert data["brands"] == []
    assert data["customer_segments"] == []
    assert data["business_categories"] == []


def test_get_company_client_not_found(client: TestClient) -> None:
    class FakeCompanyRepo:
        def get_ci_client(self, org_id: str) -> Optional[CIClient]:
            return None

        def get_company_profile(self, ci_client_id: int) -> Optional[ClientCompanyProfile]:
            return None

        def get_cs_client_image(self, org_id: str) -> Optional[str]:
            return None

        def get_customer_segments(self, tenant_schema: str) -> List[CustomerSegment]:
            return []

        def get_all_business_categories(self, tenant_schema: str) -> List[BusinessCategory]:
            return []

    class FakeBrandRepo:
        def get_brands(self, tenant_schema: str) -> List[Brand]:
            return []

        def get_product_lines_by_brand_ids(
            self, tenant_schema: str, brand_ids: List[int]
        ) -> List[ProductLine]:
            return []

        def get_business_categories_by_ids(
            self, tenant_schema: str, category_ids: List[int]
        ) -> List[BusinessCategory]:
            return []

    app.dependency_overrides[get_company_service] = lambda: CompanyService(FakeCompanyRepo(), FakeBrandRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/companies")
    assert resp.status_code == 404
    assert resp.json().get("detail") == "Client not found"
