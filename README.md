# Book-Selling App with Enhanced Wrangler Framework

This project is a book-selling web application that integrates various advanced features, including Byte Size and Time Duration handling in the Wrangler framework, full eCommerce capabilities, and advanced parsing and aggregation logic. The app is built using **React**, **Express**, **Sequelize (MySQL2)**, and integrates **GraphQL** for efficient data querying.

## Features Overview

1. **Book-Selling Web App**:
   - Fully functional eCommerce site for browsing and purchasing books.
   - User authentication and role-based access.
   - Product filtering by categories and publishers.
   - Cart and payments flow with real-time updates.
   - Add to wishlist and cart functionalities.

2. **Wrangler Framework Enhancements**:
   - Added **Byte Size** and **Time Duration** parsing and aggregation capabilities.
   - Enhanced **aggregate-stats** directive to handle Byte Size and Time Duration units (sum, average, median).
   - Unit tests for aggregation operations in the Wrangler framework.

3. **GraphQL Integration**:
   - Integrated GraphQL with **Express.js** for flexible querying and data manipulation.
   - Optimized resolvers and schema for efficient user management, book handling, and reviews.

4. **Unit Testing**:
   - Implemented JUnit tests for Byte Size and Time Duration aggregation functionality in Wrangler.
   - Extensive tests for core features like authentication, book listings, and payment flows.

## Project Setup

### 1. Fork & Clone the Repository
   - Fork the project from [GitHub](https://github.com/BlackSnow5120).
   - Clone the repository to your local machine:
     ```bash
     git clone https://github.com/BlackSnow5120/book-selling-app.git
     cd book-selling-app
     ```

### 2. Backend Setup (Express + Sequelize + GraphQL)

#### Prerequisites:
   - Node.js and npm installed
   - MySQL server running

#### Backend Installation:
   - Install dependencies:
     ```bash
     npm install
     ```
   - Create and configure the database (`MySQL`).
   - Set up `.env` for environment variables (database credentials, JWT secret).

#### Database Migration:
   - Migrate the database to create tables:
     ```bash
     npx sequelize-cli db:migrate
     ```

#### Backend Start:
   - Start the server:
     ```bash
     npm start
     ```

#### API Documentation:
   - Endpoints are available for book browsing, cart management, and order processing.
   - The **GraphQL** API is available for querying and mutations.

### 3. Frontend Setup (React)

#### Prerequisites:
   - Node.js and npm installed

#### Frontend Installation:
   - Install dependencies:
     ```bash
     npm install
     ```

#### Frontend Start:
   - Start the React development server:
     ```bash
     npm start
     ```

#### Frontend Features:
   - **Product listing** with filter options (categories, publishers).
   - **Cart management** with quantity updates and a dedicated payment flow.
   - **User authentication** and role management.
   - **Wishlists** and **book reviews** functionality.

## Wrangler Framework Enhancements

### 1. Modify ANTLR Grammar (Directives.g4)
   - Added lexer tokens for **Byte Size** and **Time Duration** units:
     - `BYTE_SIZE`: Formats like `10KB`, `500MB`, etc.
     - `TIME_DURATION`: Formats like `5s`, `2h`, `100ms`, etc.
   - Updated parser rules to handle these new tokens.
   - Regenerated the ANTLR parser using Maven:
     ```bash
     mvn clean compile
     ```

### 2. API Enhancements (wrangler-api)
   - Created `ByteSize.java` and `TimeDuration.java` classes extending `Token`.
   - Implemented conversion methods like `getBytes()` for Byte Size and `getMillis()` for Time Duration.
   - Added new `TokenTypes` to handle these units.

### 3. Core Parser Enhancements (wrangler-core)
   - Implemented methods to parse Byte Size and Time Duration arguments in the `RecipeVisitor` class.
   - Added support for these tokens in the `TokenGroup` for aggregation processing.

### 4. Implement `aggregate-stats` Directive
   - Enhanced the `aggregate-stats` directive to support Byte Size and Time Duration fields.
   - Supported aggregation operations include **sum**, **average**, and **median** for both Byte Size and Time Duration.
   - Example directive usage:
     ```java
     String[] recipe = new String[] {
       "aggregate-stats :data_transfer_size :response_time sum_size_mb sum_time_sec"
     };
     ```

### 5. Testing Wrangler Enhancements
   - **Unit Tests** for Byte Size and Time Duration parsing have been implemented.
   - **Parser Tests** validate new syntax in recipes for accurate parsing of Byte Size and Time Duration.
   - **Directive Tests** for aggregation using `TestingRig` ensure correct aggregation of Byte Size and Time Duration fields.

## Example Code Snippets

### Example of `aggregate-stats` Directive with Byte Size and Time Duration:
```java
String[] recipe = new String[] {
  "aggregate-stats :data_transfer_size :response_time sum_size_mb sum_time_sec"
};
