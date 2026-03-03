from typing import Generator, List, Optional

import pytest
from fastapi.testclient import TestClient

from database.tenant_models.models import Brand, BusinessCategory, ProductLine
from jwt_validator import validate_jwt
from main import app
from routes.brand_router import get_brand_service
from services.brand_service import BrandService


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    def override_validate_jwt() -> dict[str, str]:
        return {"orgId": "test_schema"}

    app.dependency_overrides[validate_jwt] = override_validate_jwt
    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides = {}


def make_business_category(category_id: int = 1) -> BusinessCategory:
    return BusinessCategory(
        id=category_id,
        business_category_name="Technology",
        business_category_description="Tech sector",
    )


def make_brand(brand_id: int = 5, category_id: Optional[int] = 1) -> Brand:
    return Brand(
        id=brand_id,
        brand_name="Acme",
        brand_description="A brand",
        brand_purpose="Purpose",
        brand_mission="Mission",
        brand_attributes=["innovative", "bold"],
        brand_business_category_id=category_id,
        brand_country=["United States"],
    )


def make_product_line(pl_id: int = 1, brand_id: int = 5) -> ProductLine:
    return ProductLine(
        id=pl_id,
        brand_id=brand_id,
        product_line_name="Alpha Line",
        product_line_user_benefit="Benefit A",
        product_line_value_proposition="Value A",
    )


def test_get_brands_success(client: TestClient) -> None:
    brand = make_brand()
    category = make_business_category()
    product_line = make_product_line()

    class FakeRepo:
        def get_brands(self, tenant_schema: str) -> List[Brand]:
            return [brand]

        def get_brand_by_id(self, tenant_schema: str, brand_id: int) -> Optional[Brand]:
            return brand

        def get_product_lines_by_brand_ids(
            self, tenant_schema: str, brand_ids: List[int]
        ) -> List[ProductLine]:
            return [product_line]

        def get_business_categories_by_ids(
            self, tenant_schema: str, category_ids: List[int]
        ) -> List[BusinessCategory]:
            return [category]

    app.dependency_overrides[get_brand_service] = lambda: BrandService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/brands")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1

    b = data[0]
    assert b["id"] == 5
    assert b["brand_name"] == "Acme"
    assert b["brand_attributes"] == ["innovative", "bold"]
    assert b["brand_country"] == ["United States"]

    assert b["brand_business_category"] is not None
    assert b["brand_business_category"]["business_category_name"] == "Technology"
    assert b["brand_business_category"]["name"] == "Technology"
    assert b["brand_business_category"]["description"] == "Tech sector"

    assert len(b["product_lines"]) == 1
    assert b["product_lines"][0]["product_line_name"] == "Alpha Line"
    assert b["product_lines"][0]["brand_id"] == 5


def test_get_brands_empty(client: TestClient) -> None:
    class FakeRepo:
        def get_brands(self, tenant_schema: str) -> List[Brand]:
            return []

        def get_brand_by_id(self, tenant_schema: str, brand_id: int) -> Optional[Brand]:
            return None

        def get_product_lines_by_brand_ids(
            self, tenant_schema: str, brand_ids: List[int]
        ) -> List[ProductLine]:
            return []

        def get_business_categories_by_ids(
            self, tenant_schema: str, category_ids: List[int]
        ) -> List[BusinessCategory]:
            return []

    app.dependency_overrides[get_brand_service] = lambda: BrandService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/brands")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_brand_found(client: TestClient) -> None:
    brand = make_brand(brand_id=7, category_id=None)
    product_line = make_product_line(brand_id=7)

    class FakeRepo:
        def get_brands(self, tenant_schema: str) -> List[Brand]:
            return []

        def get_brand_by_id(self, tenant_schema: str, brand_id: int) -> Optional[Brand]:
            return brand

        def get_product_lines_by_brand_ids(
            self, tenant_schema: str, brand_ids: List[int]
        ) -> List[ProductLine]:
            return [product_line]

        def get_business_categories_by_ids(
            self, tenant_schema: str, category_ids: List[int]
        ) -> List[BusinessCategory]:
            return []

    app.dependency_overrides[get_brand_service] = lambda: BrandService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/brands/7")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == 7
    assert data["brand_name"] == "Acme"
    assert data["brand_business_category"] is None
    assert len(data["product_lines"]) == 1
    assert data["product_lines"][0]["brand_id"] == 7


def test_get_brand_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_brands(self, tenant_schema: str) -> List[Brand]:
            return []

        def get_brand_by_id(self, tenant_schema: str, brand_id: int) -> Optional[Brand]:
            return None

        def get_product_lines_by_brand_ids(
            self, tenant_schema: str, brand_ids: List[int]
        ) -> List[ProductLine]:
            return []

        def get_business_categories_by_ids(
            self, tenant_schema: str, category_ids: List[int]
        ) -> List[BusinessCategory]:
            return []

    app.dependency_overrides[get_brand_service] = lambda: BrandService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/brands/999")
    assert resp.status_code == 404
    assert resp.json().get("detail") == "Brand not available"
