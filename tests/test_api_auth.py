"""
Tests for API authentication and authorization.
"""

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient
from main import app
from db.base import SessionLocal


@pytest.fixture
def client():
    """Create a FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def auth_headers(client):
    """
    Register a test user, promote to admin, and return JWT auth headers.
    Cleans up the test user after the test.
    """
    session = SessionLocal()

    client.post("/auth/register", params={
        "username": "testadmin",
        "email": "testadmin@test.com",
        "password": "testpass123",
    })

    session.execute(
        sa.text("UPDATE auth.api_user SET is_admin = true WHERE username = 'testadmin'")
    )
    session.commit()

    response = client.post("/auth/login", data={
        "username": "testadmin",
        "password": "testpass123",
    })
    token = response.json()["access_token"]

    yield {"Authorization": f"Bearer {token}"}

    session.execute(
        sa.text("DELETE FROM auth.api_user WHERE username = 'testadmin'")
    )
    session.commit()
    session.close()

def test_protected_route_without_token(client):
    """Should return 401 when no token is provided."""
    response = client.get("/employees")
    assert response.status_code == 401


def test_protected_route_with_token(client, auth_headers):
    """Should return 200 when valid token is provided."""
    response = client.get("/employees", headers=auth_headers)
    assert response.status_code == 200


def test_viewer_cannot_create_employee(client):
    """Viewer role should get 401 trying to create an employee without token."""
    response = client.post("/employees", json={
        "client_employee_id": "TEST99",
        "first_name": "Test",
        "last_name": "User",
    })
    assert response.status_code == 401


def test_admin_can_create_and_delete_employee(client, auth_headers):
    """Admin should be able to create and soft-delete an employee."""
    session = SessionLocal()
    session.execute(
        sa.text("DELETE FROM silver.employee WHERE client_employee_id = 'TEST99'")
    )
    session.commit()
    session.close()
    
    response = client.post("/employees/", headers=auth_headers, json={
        "client_employee_id": "TEST99",
        "first_name": "Test",
        "last_name": "User",
        "active_status": True,
        "hire_date": "2020-01-01",
    })
    assert response.status_code == 201
    employee_id = response.json()["id"]

    response = client.delete(f"/employees/{employee_id}", headers=auth_headers)
    assert response.status_code == 200