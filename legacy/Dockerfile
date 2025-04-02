# Use the official TensorFlow image as a parent image
FROM tensorflow/tensorflow:latest-jupyter

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the project files into the container at /usr/src/app
COPY ./legacy /usr/src/app

# update pip version
RUN python3 -m pip install --upgrade pip

# (Optional) Install any dependencies
RUN pip install .

# Make port 8888 available to the world outside this container
EXPOSE 8888

# Run Jupyter Notebook when the container launches
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root"]
