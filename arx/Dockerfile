FROM openjdk:17-jdk-alpine

WORKDIR /app

# Copy the jar file from the target directory of your Maven build into the container
COPY arx_v5.jar .
COPY Medical_Data_new.csv /home/kailash/Desktop/arx_anonymization/arx/Medical_Data_new.csv
# Expose the port your application runs on
EXPOSE 8070

# Command to run the jar file
ENTRYPOINT ["java", "-jar", "arx_v5.jar"]
