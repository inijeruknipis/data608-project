FROM public.ecr.aws/lambda/python:3.14

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy function
COPY lambda_function.py .

# Set handler
CMD ["lambda_function.lambda_handler"]

//1
//2
//3