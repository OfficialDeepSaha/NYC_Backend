from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, extract
from database import SessionLocal, Dataset
from typing import List, Dict, Any
import json
from datetime import datetime, timedelta

app = FastAPI(title="NYC Datasets Dashboard API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def read_root():
    return {"message": "NYC Datasets Dashboard API"}

@app.get("/api/stats/overview")
def get_overview_stats(db: Session = Depends(get_db)):
    """Get overview statistics for the dashboard"""
    total_datasets = db.query(Dataset).count()
    total_page_views = db.query(func.sum(Dataset.page_views_total)).scalar() or 0
    total_downloads = db.query(func.sum(Dataset.download_count)).scalar() or 0
    avg_weekly_views = db.query(func.avg(Dataset.page_views_last_week)).scalar() or 0
    
    return {
        "total_datasets": total_datasets,
        "total_page_views": int(total_page_views),
        "total_downloads": int(total_downloads),
        "avg_weekly_views": round(float(avg_weekly_views), 2)
    }

@app.get("/api/datasets/top-viewed")
def get_top_viewed_datasets(limit: int = 10, db: Session = Depends(get_db)):
    """Get top viewed datasets"""
    datasets = db.query(Dataset).order_by(desc(Dataset.page_views_total)).limit(limit).all()
    return [
        {
            "id": d.id,
            "name": d.name,
            "page_views_total": d.page_views_total,
            "download_count": d.download_count,
            "agency": d.dataset_information_agency,
            "category": d.domain_category
        }
        for d in datasets
    ]

@app.get("/api/datasets/top-downloaded")
def get_top_downloaded_datasets(limit: int = 10, db: Session = Depends(get_db)):
    """Get top downloaded datasets"""
    datasets = db.query(Dataset).order_by(desc(Dataset.download_count)).limit(limit).all()
    return [
        {
            "id": d.id,
            "name": d.name,
            "page_views_total": d.page_views_total,
            "download_count": d.download_count,
            "agency": d.dataset_information_agency,
            "category": d.domain_category
        }
        for d in datasets
    ]

@app.get("/api/analytics/by-agency")
def get_analytics_by_agency(db: Session = Depends(get_db)):
    """Get analytics grouped by agency"""
    results = db.query(
        Dataset.dataset_information_agency,
        func.count(Dataset.id).label('dataset_count'),
        func.sum(Dataset.page_views_total).label('total_views'),
        func.sum(Dataset.download_count).label('total_downloads')
    ).filter(
        Dataset.dataset_information_agency.isnot(None)
    ).group_by(
        Dataset.dataset_information_agency
    ).order_by(
        desc('total_views')
    ).limit(15).all()
    
    return [
        {
            "agency": r.dataset_information_agency,
            "dataset_count": r.dataset_count,
            "total_views": int(r.total_views or 0),
            "total_downloads": int(r.total_downloads or 0)
        }
        for r in results
    ]

@app.get("/api/analytics/by-category")
def get_analytics_by_category(db: Session = Depends(get_db)):
    """Get analytics grouped by category"""
    results = db.query(
        Dataset.domain_category,
        func.count(Dataset.id).label('dataset_count'),
        func.sum(Dataset.page_views_total).label('total_views'),
        func.sum(Dataset.download_count).label('total_downloads')
    ).filter(
        Dataset.domain_category.isnot(None)
    ).group_by(
        Dataset.domain_category
    ).order_by(
        desc('total_views')
    ).all()
    
    return [
        {
            "category": r.domain_category,
            "dataset_count": r.dataset_count,
            "total_views": int(r.total_views or 0),
            "total_downloads": int(r.total_downloads or 0)
        }
        for r in results
    ]

@app.get("/api/analytics/publication-timeline")
def get_publication_timeline(db: Session = Depends(get_db)):
    """Get publication timeline by year"""
    results = db.query(
        extract('year', Dataset.publication_date).label('year'),
        func.count(Dataset.id).label('count')
    ).filter(
        Dataset.publication_date.isnot(None)
    ).group_by(
        extract('year', Dataset.publication_date)
    ).order_by(
        'year'
    ).all()
    
    return [
        {
            "year": int(r.year),
            "count": r.count
        }
        for r in results if r.year and r.year >= 2000
    ]

@app.get("/api/analytics/engagement-metrics")
def get_engagement_metrics(db: Session = Depends(get_db)):
    """Get engagement metrics comparison"""
    results = db.query(
        Dataset.name,
        Dataset.page_views_last_week,
        Dataset.page_views_last_month,
        Dataset.page_views_total,
        Dataset.download_count,
        Dataset.domain_category
    ).filter(
        Dataset.page_views_total > 0
    ).order_by(
        desc(Dataset.page_views_total)
    ).limit(20).all()
    
    return [
        {
            "name": r.name[:50] + "..." if len(r.name) > 50 else r.name,
            "weekly_views": r.page_views_last_week or 0,
            "monthly_views": r.page_views_last_month or 0,
            "total_views": r.page_views_total or 0,
            "downloads": r.download_count or 0,
            "category": r.domain_category
        }
        for r in results
    ]

@app.get("/api/datasets/search")
def search_datasets(q: str = "", category: str = "", agency: str = "", limit: int = 50, db: Session = Depends(get_db)):
    """Search datasets with filters"""
    query = db.query(Dataset)
    
    if q:
        query = query.filter(Dataset.name.ilike(f"%{q}%"))
    
    if category:
        query = query.filter(Dataset.domain_category == category)
    
    if agency:
        query = query.filter(Dataset.dataset_information_agency == agency)
    
    datasets = query.order_by(desc(Dataset.page_views_total)).limit(limit).all()
    
    return [
        {
            "id": d.id,
            "name": d.name,
            "description": d.description[:200] + "..." if d.description and len(d.description) > 200 else d.description,
            "agency": d.dataset_information_agency,
            "category": d.domain_category,
            "page_views_total": d.page_views_total,
            "download_count": d.download_count,
            "publication_date": d.publication_date.isoformat() if d.publication_date else None,
            "link": d.link
        }
        for d in datasets
    ]

@app.get("/api/filters/categories")
def get_categories(db: Session = Depends(get_db)):
    """Get all available categories"""
    categories = db.query(Dataset.domain_category).filter(
        Dataset.domain_category.isnot(None)
    ).distinct().all()
    return [c[0] for c in categories if c[0]]

@app.get("/api/filters/agencies")
def get_agencies(db: Session = Depends(get_db)):
    """Get all available agencies"""
    agencies = db.query(Dataset.dataset_information_agency).filter(
        Dataset.dataset_information_agency.isnot(None)
    ).distinct().all()
    return [a[0] for a in agencies if a[0]]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)