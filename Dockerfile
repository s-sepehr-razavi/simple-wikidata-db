# Step 1: Use an official Python runtime as the base image
FROM python

# Step 2: Set the working directory in the container
WORKDIR /app

# Step 3: Copy the requirements file to the container
COPY requirements.txt ./

# Step 4: Install dependencies from the requirements file
RUN pip install --no-cache-dir -r requirements.txt

# Step 5: Copy the rest of the application code to the container
COPY . .

# Step 6: Expose any necessary ports (e.g., if running a web app)
# EXPOSE 5000  # Uncomment this if your Python app is a web server
