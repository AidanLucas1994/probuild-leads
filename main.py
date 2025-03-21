from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify
from datetime import datetime, timedelta
from collections import Counter
import random
import csv
import io
import os
import sys
import logging
from models import db, Lead
from fetch_permits import fetch_permit_data, transform_permit_data
import pandas as pd
import numpy as np

# Configure logging
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Get the absolute path of the current directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Ensure template and static directories exist
template_dir = os.path.join(BASE_DIR, 'templates')
static_dir = os.path.join(BASE_DIR, 'static')

try:
    os.makedirs(template_dir, exist_ok=True)
    os.makedirs(static_dir, exist_ok=True)
    logger.info(f"Template directory: {template_dir}")
    logger.info(f"Static directory: {static_dir}")
except Exception as e:
    logger.error(f"Error creating directories: {e}")
    # Don't exit, just log the error
    logger.warning("Continuing despite directory creation error")

# Initialize Flask app with explicit template and static folder paths
app = Flask(__name__,
            template_folder=template_dir,
            static_folder=static_dir)

# Configure SQLAlchemy
database_url = os.environ.get('DATABASE_URL')
if database_url and database_url.startswith('postgres://'):
    # Render uses postgres:// but SQLAlchemy requires postgresql://
    database_url = database_url.replace('postgres://', 'postgresql://', 1)
    logger.info("Using PostgreSQL database")
else:
    # Default to SQLite for local development
    database_url = f'sqlite:///{os.path.join(BASE_DIR, "leads.db")}'
    logger.info("Using SQLite database")

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Use environment variable for secret key with a fallback
app.secret_key = os.environ.get('SECRET_KEY', 'dev-key-please-change-in-production')
# Disable debug mode in production
app.debug = os.environ.get('FLASK_ENV') == 'development'

# Initialize the database
db.init_app(app)

# Create database tables
try:
    with app.app_context():
        db.create_all()
        logger.info("Database tables created successfully")
except Exception as e:
    logger.error(f"Error creating database tables: {e}")

def generate_sample_data(num_samples=25):
    """Generate simulated building permit data."""
    project_types = [
        'Residential Building (House)',
        'Commercial Building',
        'Industrial Building',
        'Residential Alteration',
        'Non-Residential Alteration'
    ]
    
    work_types = [
        'New Construction',
        'Addition to Building',
        'Interior Alteration',
        'Exterior Alteration',
        'Renovation'
    ]
    
    sub_work_types = {
        'Residential Building (House)': ['Single Detached Dwelling', 'Semi-Detached Dwelling', 'Townhouse'],
        'Commercial Building': ['Office Building', 'Retail Store', 'Restaurant', 'Hotel'],
        'Industrial Building': ['Warehouse', 'Manufacturing Facility', 'Distribution Center'],
        'Residential Alteration': ['Kitchen Renovation', 'Bathroom Renovation', 'Basement Finishing'],
        'Non-Residential Alteration': ['Office Renovation', 'Store Renovation', 'Industrial Upgrade']
    }
    
    locations = [
        'Toronto, ON', 'Vancouver, BC', 'Montreal, QC', 'Calgary, AB',
        'Ottawa, ON', 'Edmonton, AB', 'Winnipeg, MB', 'Quebec City, QC',
        'Hamilton, ON', 'Victoria, BC', 'Halifax, NS', 'Saskatoon, SK'
    ]
    
    statuses = {
        'Pending': 0.2,
        'Approved': 0.4,
        'In Review': 0.2,
        'Closed': 0.15,
        'Expired': 0.05
    }
    
    # Generate random dates within the last 90 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    sample_data = []
    for i in range(num_samples):
        # Select project type and related sub work type
        project_type = random.choice(project_types)
        sub_work_type = random.choice(sub_work_types[project_type])
        
        # Generate dates
        submission_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Status based on weighted probabilities
        status = random.choices(list(statuses.keys()), list(statuses.values()))[0]
        
        # Generate issue date based on status
        issue_date = None
        if status in ['Approved', 'Closed']:
            issue_date = submission_date + timedelta(days=random.randint(5, 15))
        
        # Generate expiry date based on status
        expiry_date = None
        if status != 'Expired':
            expiry_date = submission_date + timedelta(days=365)
        else:
            expiry_date = submission_date + timedelta(days=random.randint(30, 60))
        
        # Generate coordinates based on location
        coordinates = {
            'Toronto, ON': (43.6532, -79.3832),
            'Vancouver, BC': (49.2827, -123.1207),
            'Montreal, QC': (45.5017, -73.5673),
            'Calgary, AB': (51.0447, -114.0719),
            'Ottawa, ON': (45.4215, -75.6972),
            'Edmonton, AB': (53.5461, -113.4938),
            'Winnipeg, MB': (49.8951, -97.1384),
            'Quebec City, QC': (46.8139, -71.2080),
            'Hamilton, ON': (43.2557, -79.8711),
            'Victoria, BC': (48.4284, -123.3656),
            'Halifax, NS': (44.6488, -63.5752),
            'Saskatoon, SK': (52.1332, -106.6700)
        }
        
        location = random.choice(locations)
        base_lat, base_lon = coordinates[location]
        lat = base_lat + random.uniform(-0.05, 0.05)
        lon = base_lon + random.uniform(-0.05, 0.05)
        
        # Generate construction value based on project type
        base_values = {
            'Residential Building (House)': (500000, 2000000),
            'Commercial Building': (1000000, 5000000),
            'Industrial Building': (2000000, 10000000),
            'Residential Alteration': (50000, 300000),
            'Non-Residential Alteration': (100000, 1000000)
        }
        min_value, max_value = base_values[project_type]
        construction_value = random.randint(min_value, max_value)
        
        # Generate units based on project type
        total_units = 1
        units_created = 0
        if 'Residential' in project_type:
            if 'House' in project_type:
                total_units = random.randint(1, 3)
            else:
                total_units = 1
            units_created = random.randint(0, total_units)
        
        lead = Lead(
            permit_number=f'BP{datetime.now().year}{i+1:04d}',
            project_type=project_type,
            permit_status=status,
            permit_description=f'New {sub_work_type.lower()} project',
            submission_date=submission_date,
            issue_date=issue_date,
            expiry_date=expiry_date,
            location=location,
            work_type=random.choice(work_types),
            sub_work_type=sub_work_type,
            construction_value=construction_value,
            total_units=total_units,
            units_created=units_created,
            legal_description=f'PLAN {random.randint(1000, 9999)} LOT {random.randint(1, 999)}',
            latitude=lat,
            longitude=lon,
            owner=f'Owner {i+1}',
            applicant=f'Applicant {i+1}',
            contractor=f'Contractor {i+1}',
            contractor_contact=f'contact{i+1}@example.com'
        )
        sample_data.append(lead)
    
    return sample_data

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        project_type = request.form.get('project_type')
        location = request.form.get('location')
        value_range = request.form.get('value_range')

        if not all([project_type, location, value_range]):
            flash('All fields are required!', 'error')
            return redirect(url_for('index'))

        # Create a new lead
        lead = Lead(
            project_type=project_type,
            location=location,
            value_range=f"CAD {value_range}",
            submission_date=datetime.now()
        )
        db.session.add(lead)
        db.session.commit()

        flash('Lead submitted successfully!', 'success')
        return redirect(url_for('dashboard'))

    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    # Get filter parameters
    project_type_filter = request.args.get('project_type')
    location_filter = request.args.get('location')
    status_filter = request.args.get('status')
    work_type_filter = request.args.get('work_type')
    min_value = request.args.get('min_value', type=float)
    max_value = request.args.get('max_value', type=float)
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    # Start with base query
    query = Lead.query

    # Apply filters
    if project_type_filter:
        query = query.filter_by(project_type=project_type_filter)
    if location_filter:
        query = query.filter_by(location=location_filter)
    if status_filter:
        query = query.filter_by(permit_status=status_filter)
    if work_type_filter:
        query = query.filter_by(work_type=work_type_filter)
    if min_value is not None:
        query = query.filter(Lead.construction_value >= min_value)
    if max_value is not None:
        query = query.filter(Lead.construction_value <= max_value)
    if date_from:
        query = query.filter(Lead.submission_date >= datetime.strptime(date_from, '%Y-%m-%d'))
    if date_to:
        query = query.filter(Lead.submission_date <= datetime.strptime(date_to, '%Y-%m-%d'))

    # Execute query
    filtered_leads = query.order_by(Lead.submission_date.desc()).all()
    
    # If no leads exist, generate and add sample data
    if not filtered_leads and not any([project_type_filter, location_filter, status_filter, 
                                     work_type_filter, min_value, max_value, date_from, date_to]):
        sample_leads = generate_sample_data()
        for lead in sample_leads:
            db.session.add(lead)
        db.session.commit()
        filtered_leads = Lead.query.order_by(Lead.submission_date.desc()).all()

    # Calculate statistics
    total_leads = len(filtered_leads)
    total_value = sum(lead.construction_value for lead in filtered_leads)
    avg_value = total_value / total_leads if total_leads > 0 else 0
    
    # Count by various categories
    project_types = Counter(lead.project_type for lead in filtered_leads)
    work_types = Counter(lead.work_type for lead in filtered_leads)
    statuses = Counter(lead.permit_status for lead in filtered_leads)
    
    # Get unique values for filter dropdowns from all leads
    all_leads = Lead.query.all()
    available_filters = {
        'project_types': sorted(set(lead.project_type for lead in all_leads)),
        'locations': sorted(set(lead.location for lead in all_leads)),
        'statuses': sorted(set(lead.permit_status for lead in all_leads)),
        'work_types': sorted(set(lead.work_type for lead in all_leads)),
        'value_range': {
            'min': min(lead.construction_value for lead in all_leads) if all_leads else 0,
            'max': max(lead.construction_value for lead in all_leads) if all_leads else 0
        }
    }
    
    return render_template('dashboard.html',
                         leads=filtered_leads,
                         total_leads=total_leads,
                         total_value=f"CAD ${total_value:,.2f}",
                         avg_value=f"CAD ${avg_value:,.2f}",
                         project_types=dict(project_types),
                         work_types=dict(work_types),
                         statuses=dict(statuses),
                         available_filters=available_filters,
                         request=request)

@app.route('/download-csv')
def download_csv():
    # Get filter parameters (same as dashboard)
    project_type_filter = request.args.get('project_type')
    location_filter = request.args.get('location')
    status_filter = request.args.get('status')
    work_type_filter = request.args.get('work_type')
    min_value = request.args.get('min_value', type=float)
    max_value = request.args.get('max_value', type=float)
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    # Start with base query
    query = Lead.query

    # Apply filters
    if project_type_filter:
        query = query.filter_by(project_type=project_type_filter)
    if location_filter:
        query = query.filter_by(location=location_filter)
    if status_filter:
        query = query.filter_by(permit_status=status_filter)
    if work_type_filter:
        query = query.filter_by(work_type=work_type_filter)
    if min_value is not None:
        query = query.filter(Lead.construction_value >= min_value)
    if max_value is not None:
        query = query.filter(Lead.construction_value <= max_value)
    if date_from:
        query = query.filter(Lead.submission_date >= datetime.strptime(date_from, '%Y-%m-%d'))
    if date_to:
        query = query.filter(Lead.submission_date <= datetime.strptime(date_to, '%Y-%m-%d'))

    # Execute query
    filtered_leads = query.order_by(Lead.submission_date.desc()).all()

    # Create a StringIO object to write CSV data
    si = io.StringIO()
    writer = csv.writer(si)
    
    # Write headers
    headers = [
        'Permit Number',
        'Project Type',
        'Status',
        'Description',
        'Submission Date',
        'Issue Date',
        'Expiry Date',
        'Location',
        'Work Type',
        'Sub Work Type',
        'Construction Value',
        'Total Units',
        'Units Created',
        'Legal Description',
        'Latitude',
        'Longitude',
        'Owner',
        'Applicant',
        'Contractor',
        'Contractor Contact'
    ]
    writer.writerow(headers)
    
    # Write data rows
    for lead in filtered_leads:
        row = [
            lead.permit_number,
            lead.project_type,
            lead.permit_status,
            lead.permit_description,
            lead.submission_date.strftime('%Y-%m-%d %H:%M') if lead.submission_date else '',
            lead.issue_date.strftime('%Y-%m-%d %H:%M') if lead.issue_date else '',
            lead.expiry_date.strftime('%Y-%m-%d %H:%M') if lead.expiry_date else '',
            lead.location,
            lead.work_type,
            lead.sub_work_type,
            f"${lead.construction_value:,.2f}" if lead.construction_value else '',
            lead.total_units,
            lead.units_created,
            lead.legal_description,
            lead.latitude,
            lead.longitude,
            lead.owner,
            lead.applicant,
            lead.contractor,
            lead.contractor_contact
        ]
        writer.writerow(row)
    
    # Create the response
    output = si.getvalue()
    si.close()
    
    # Create a new StringIO object with the output
    mem = io.BytesIO()
    mem.write(output.encode('utf-8'))
    mem.seek(0)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return send_file(
        mem,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'construction_leads_{timestamp}.csv'
    )

@app.route('/api/leads')
def api_leads():
    # Get filter parameters from query string
    project_type = request.args.get('project_type')
    location = request.args.get('location')
    
    # Start with base query
    query = Lead.query

    # Apply filters
    if project_type:
        query = query.filter_by(project_type=project_type)
    if location:
        query = query.filter_by(location=location)

    # Execute query
    filtered_leads = query.order_by(Lead.submission_date.desc()).all()
    
    # Format the response data
    response_data = [{
        'project_type': lead.project_type,
        'location': lead.location,
        'construction_value': lead.value_range,
        'permit_date': lead.submission_date.strftime('%Y-%m-%d %H:%M'),
    } for lead in filtered_leads]
    
    # Return JSON response with metadata
    return jsonify({
        'status': 'success',
        'total_results': len(response_data),
        'filters_applied': {
            'project_type': project_type,
            'location': location
        },
        'data': response_data
    })

@app.route('/api/leads/<int:lead_id>')
def get_lead_details(lead_id):
    lead = Lead.query.get_or_404(lead_id)
    return jsonify(lead.to_dict())

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'template_dir': os.path.exists(template_dir),
        'static_dir': os.path.exists(static_dir),
        'database': 'connected' if db.engine.execute('SELECT 1').scalar() else 'error'
    })

@app.route('/regenerate-data')
def regenerate_data():
    """Route to manually regenerate sample data."""
    try:
        # Clear existing data
        db.session.query(Lead).delete()
        db.session.commit()
        
        # Generate and add new sample data
        sample_leads = generate_sample_data(25)  # Generate 25 sample leads
        for lead in sample_leads:
            db.session.add(lead)
        db.session.commit()
        
        flash('Sample data regenerated successfully!', 'success')
    except Exception as e:
        logger.error(f"Error regenerating sample data: {e}")
        flash('Error regenerating sample data.', 'error')
        db.session.rollback()
    
    return redirect(url_for('dashboard'))

@app.route('/fetch-permit-data')
def fetch_permits():
    """
    Fetch and transform permit data from the ArcGIS API.
    Returns JSON with transformed permit data filtered to the last 12 months.
    """
    try:
        logger.info("Fetching permit data from API...")
        raw_df = fetch_permit_data()
        
        if raw_df is None:
            logger.error("Failed to fetch permit data from API")
            return jsonify({
                'status': 'error',
                'message': 'Failed to fetch permit data from API',
                'error_type': 'api_error'
            }), 500

        # Transform the data
        logger.info("Transforming permit data...")
        result = transform_permit_data(raw_df)
        
        if result is None:
            logger.error("Failed to transform permit data")
            return jsonify({
                'status': 'error',
                'message': 'Failed to transform permit data',
                'error_type': 'transformation_error'
            }), 500

        logger.info(f"Successfully processed permit data")
        return jsonify(result)

    except Exception as e:
        logger.error(f"Unexpected error in fetch-permit-data route: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'An unexpected error occurred while processing permit data',
            'error_type': 'unexpected_error',
            'error_details': str(e)
        }), 500

@app.route('/leads')
def get_leads_by_contractor():
    """
    Fetch permit data and organize it by contractor type with pagination support.
    Query parameters:
        page (int): Page number (default: 1)
        limit (int): Number of records per page (default: 50)
    Returns a JSON response with paginated permits grouped by contractor type and summary statistics.
    """
    try:
        logger.info("=== Starting /leads request ===")
        
        # Log raw query parameters
        logger.info("Raw query parameters: %s", dict(request.args))
        
        # Get and validate pagination parameters
        try:
            page = int(request.args.get('page', 1))
            limit = int(request.args.get('limit', 50))
            
            logger.info("Parsed pagination parameters: page=%d, limit=%d", page, limit)
            
            # Validate parameters
            if page < 1:
                logger.error("Invalid page number: %d (must be >= 1)", page)
                raise ValueError("Page number must be greater than 0")
            if limit < 1 or limit > 100:
                logger.error("Invalid limit: %d (must be between 1 and 100)", limit)
                raise ValueError("Limit must be between 1 and 100")
                
        except (TypeError, ValueError) as e:
            logger.error("Pagination parameter validation failed: %s", str(e))
            return jsonify({
                'status': 'error',
                'message': 'Invalid pagination parameters',
                'error_type': 'validation_error',
                'error_details': str(e)
            }), 400
        
        # Get multi-select filter parameters
        contractor_types = request.args.getlist('contractor_type[]')
        statuses = request.args.getlist('status[]')
        work_types = request.args.getlist('work_type[]')
        min_value = request.args.get('min_value')
        max_value = request.args.get('max_value')
        
        logger.info("Filter parameters: contractor_types=%s, statuses=%s, work_types=%s, min_value=%s, max_value=%s",
                   contractor_types, statuses, work_types, min_value, max_value)
        
        # Fetch permit data
        logger.info("Fetching permit data from API...")
        raw_df = fetch_permit_data()
        if raw_df is None:
            logger.error("Failed to fetch permit data from API")
            return jsonify({
                'status': 'error',
                'message': 'Failed to fetch permit data',
                'error_type': 'data_fetch_error'
            }), 500

        # Transform the data
        logger.info("Transforming permit data...")
        result = transform_permit_data(raw_df)
        if result['status'] != 'success':
            logger.error("Data transformation failed: %s", result.get('message', 'Unknown error'))
            return jsonify(result), 400

        # Extract permits from the result
        all_permits = result['permits']
        logger.info("Total permits before filtering: %d", len(all_permits))
        
        # Apply filters if provided
        filtered_permits = all_permits
        
        # Filter by contractor types (if specified)
        if contractor_types:
            filtered_permits = [p for p in filtered_permits 
                              if p['Contractor Type'] in contractor_types]
            logger.info("After contractor type filter: %d permits", len(filtered_permits))
            
        # Filter by statuses (if specified)
        if statuses:
            filtered_permits = [p for p in filtered_permits 
                              if p['Status'].lower() in [s.lower() for s in statuses]]
            logger.info("After status filter: %d permits", len(filtered_permits))
            
        # Filter by work types (if specified)
        if work_types:
            filtered_permits = [p for p in filtered_permits 
                              if p['Work Type'] in work_types]
            logger.info("After work type filter: %d permits", len(filtered_permits))
        
        # Filter by value range
        if min_value:
            try:
                min_val = float(min_value)
                filtered_permits = [p for p in filtered_permits 
                                 if float(p['Construction Value'].replace('$', '').replace(',', '')) >= min_val]
                logger.info("After min value filter: %d permits", len(filtered_permits))
            except ValueError as e:
                logger.warning("Invalid min_value parameter: %s", str(e))
                
        if max_value:
            try:
                max_val = float(max_value)
                filtered_permits = [p for p in filtered_permits 
                                 if float(p['Construction Value'].replace('$', '').replace(',', '')) <= max_val]
                logger.info("After max value filter: %d permits", len(filtered_permits))
            except ValueError as e:
                logger.warning("Invalid max_value parameter: %s", str(e))
        
        # Calculate total pages
        total_permits = len(filtered_permits)
        total_pages = max(1, (total_permits + limit - 1) // limit)
        
        logger.info("Pagination calculation: total_permits=%d, total_pages=%d", total_permits, total_pages)
        
        # Validate page number against total pages
        if page > total_pages:
            logger.error("Page number %d exceeds total pages %d", page, total_pages)
            return jsonify({
                'status': 'error',
                'message': f'Page {page} exceeds total pages ({total_pages})',
                'error_type': 'pagination_error',
                'pagination': {
                    'total_pages': total_pages,
                    'total_records': total_permits,
                    'current_page': page,
                    'limit': limit
                }
            }), 400
        
        # Calculate pagination indices
        start_idx = (page - 1) * limit
        end_idx = min(start_idx + limit, total_permits)
        logger.info("Pagination indices: start=%d, end=%d", start_idx, end_idx)
        
        # Group permits by contractor type
        categorized_permits = {}
        contractor_counts = {}
        contractor_values = {}
        
        # First pass: count totals for each contractor type
        for permit in filtered_permits:
            contractor_type = permit['Contractor Type']
            if contractor_type not in categorized_permits:
                categorized_permits[contractor_type] = []
                contractor_counts[contractor_type] = 0
                contractor_values[contractor_type] = 0.0
            
            contractor_counts[contractor_type] += 1
            value = float(permit['Construction Value'].replace('$', '').replace(',', ''))
            contractor_values[contractor_type] += value
        
        logger.info("Contractor distribution: %s", contractor_counts)
        
        # Second pass: paginate permits
        paginated_permits = {ctype: [] for ctype in categorized_permits.keys()}
        current_count = 0
        
        for permit in filtered_permits:
            if current_count >= end_idx:
                break
                
            if current_count >= start_idx:
                contractor_type = permit['Contractor Type']
                paginated_permits[contractor_type].append(permit)
                
            current_count += 1

        # Format contractor values
        formatted_contractor_values = {
            ctype: "${:,.2f}".format(value)
            for ctype, value in contractor_values.items()
        }

        # Prepare the response
        response = {
            'status': 'success',
            'summary': {
                'total_permits': total_permits,
                'contractor_distribution': contractor_counts,
                'total_value_by_contractor': formatted_contractor_values,
                'date_range': result['summary']['date_range']
            },
            'pagination': {
                'page': page,
                'limit': limit,
                'total_pages': total_pages,
                'total_records': total_permits,
                'has_next': page < total_pages,
                'has_prev': page > 1,
                'next_page': page + 1 if page < total_pages else None,
                'prev_page': page - 1 if page > 1 else None,
                'start_index': start_idx + 1,
                'end_index': end_idx
            },
            'categorized_permits': paginated_permits,
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'source': 'Building Permits API',
                'categories': list(categorized_permits.keys())
            }
        }

        logger.info("Response summary: total_permits=%d, page=%d/%d, permits_in_response=%d",
                   total_permits, page, total_pages,
                   sum(len(permits) for permits in paginated_permits.values()))
        logger.info("=== Completed /leads request ===")
        
        return jsonify(response)

    except Exception as e:
        logger.error("Unexpected error in /leads route: %s", str(e), exc_info=True)
        return jsonify({
            'status': 'error',
            'message': 'An error occurred while processing the leads',
            'error_type': 'processing_error',
            'error_details': str(e)
        }), 500

if __name__ == '__main__':
    # Log startup information
    logger.info(f"Starting application in {os.environ.get('FLASK_ENV', 'production')} mode")
    logger.info(f"Template directory exists: {os.path.exists(template_dir)}")
    logger.info(f"Static directory exists: {os.path.exists(static_dir)}")
    
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)