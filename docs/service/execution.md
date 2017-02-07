# Directive Execution

**Executing Directives**
----
  Applies directives on the data in the workspace.

* **URL**

  `workspaces/:workspaceid/execute`

* **Method:**
  
  `GET`
  
*  **URL Params**

The directives to be executed are passed as query arguments. For multiple directives to be executed, they are passed as multiple
query arguments. 

 **Required:**
 
   `directive=[encoded directive]`

   **Optional:**
 
   `limit=[numeric]`

* **Data Params**

  _Not Applicable_

* **Success Response:**

  * **Code:** 200 </br> **Content:** 
  ```
    { 
      'status' : 200,
      'message' : 'Success',
      'items' : <count of records>,
      'header' : [ 'header-1', 'header-2', ..., 'header-n' ],
      'value' : {
        { processed record - 1},
        { processed record - 2},
        . . .
        { processed record - n}
      } 
    }
  ```
 
* **Error Response:**

  * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    { 
      'status' : 500,
      'message' : "<appropriate error message>"
    }
  ```
  **OR**
  
   * **Code:** 500 Server Error <br />
    **Content:** 
  ```
    Unable to route to service <url>
  ```
  
* **Sample Call:**

  ```
    $.ajax({
      url: "${base-url}/workspaces/${workspace}/execute",
      data: {
        'directive': <directive-1>,
        'directive': <directive-2>,
        ...
        'directive': <directive-k>
        'limit': <count>
      }
      cache: false
      type : "GET",
      success : function(r) {
        console.log(r);
      }
    });  
  ```
