#include "utiles.h"
#include "connutil.h"
#include <libssh/libssh.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <libxml/parser.h>
#include <libxml/tree.h>

#define _POSIX_C_SOURCE 200809L

/*
 * Forcing a function not to be inlined can be useful if it's the slow path of
 * a performance-critical function, or should be visible in profiles to allow
 * for proper cost attribution.  Note that unlike the pg_attribute_XXX macros
 * above, this should be placed before the function's return type and name.
 */
/* GCC and Sunpro support noinline via __attribute__ */
#if (defined(__GNUC__) && __GNUC__ > 2) || defined(__SUNPRO_C)
#define pg_noinline __attribute__((noinline))
/* msvc via declspec */
#elif defined(_MSC_VER)
#define pg_noinline __declspec(noinline)
#else
#define pg_noinline
#endif



static char *
simple_prompt_extended(const char *prompt, bool echo,
					   PromptInterruptContext *prompt_ctx);
char *
apache_strdup(const char *in);
void printBorder(const char *start, const char *end, const char *color);
void printTextBlock(const char *text, const char *textColor, const char *borderColor);




#if defined(WIN32) && !defined(__CYGWIN__)
#undef system
#undef popen
FILE *
win32_popen(const char *command, const char *type)
{
        size_t          cmdlen = strlen(command);
        char       *buf;
        int                     save_errno;
        FILE       *res;

        /*
         * Create a malloc'd copy of the command string, enclosed with an extra
         * pair of quotes
         */
        buf = malloc(cmdlen + 2 + 1);
        if (buf == NULL)
        {
                errno = ENOMEM;
                return NULL;
        }
        buf[0] = '"';
        memcpy(&buf[1], command, cmdlen);
        buf[cmdlen + 1] = '"';
        buf[cmdlen + 2] = '\0';

        res = _popen(buf, type);

        save_errno = errno;
        free(buf);
        errno = save_errno;

        return res;
}
#endif

/*
 * strip_crlf -- Remove any trailing newline and carriage return
 *
 * Removes any trailing newline and carriage return characters (\r on
 * Windows) in the input string, zero-terminating it.
 *
 * The passed in string must be zero-terminated.  This function returns
 * the new length of the string.
 */
int
strip_crlf(char *str)
{
        int                     len = strlen(str);

        while (len > 0 && (str[len - 1] == '\n' ||
                                           str[len - 1] == '\r'))
                str[--len] = '\0';

        return len;
}

/*
 * simple_prompt
 *
 * Generalized function especially intended for reading in usernames and
 * passwords interactively.  Reads from /dev/tty or stdin/stderr.
 *
 * prompt:		The prompt to print, or NULL if none (automatically localized)
 * echo:		Set to false if you want to hide what is entered (for passwords)
 *
 * The input (without trailing newline) is returned as a malloc'd string.
 * Caller is responsible for freeing it when done.
 */
char *
simple_prompt(const char *prompt, bool echo)
{
	return simple_prompt_extended(prompt, echo, NULL);
}

/*
 * simple_prompt_extended
 *
 * This is the same as simple_prompt(), except that prompt_ctx can
 * optionally be provided to allow this function to be canceled via an
 * existing SIGINT signal handler that will longjmp to the specified place
 * only when *(prompt_ctx->enabled) is true.  If canceled, this function
 * returns an empty string, and prompt_ctx->canceled is set to true.
 */
static char *
simple_prompt_extended(const char *prompt, bool echo,
					   PromptInterruptContext *prompt_ctx)
{
	char	   *result;
	FILE	   *termin,
			   *termout;
	size_t len = 0;
	ssize_t nread;  // Use ssize_t (signed size_t)
#if defined(HAVE_TERMIOS_H)
	struct termios t_orig,
				t;
#elif defined(WIN32)
	HANDLE		t = NULL;
	DWORD		t_orig = 0;
#endif

#ifdef WIN32

	/*
	 * A Windows console has an "input code page" and an "output code page";
	 * these usually match each other, but they rarely match the "Windows ANSI
	 * code page" defined at system boot and expected of "char *" arguments to
	 * Windows API functions.  The Microsoft CRT write() implementation
	 * automatically converts text between these code pages when writing to a
	 * console.  To identify such file descriptors, it calls GetConsoleMode()
	 * on the underlying HANDLE, which in turn requires GENERIC_READ access on
	 * the HANDLE.  Opening termout in mode "w+" allows that detection to
	 * succeed.  Otherwise, write() would not recognize the descriptor as a
	 * console, and non-ASCII characters would display incorrectly.
	 *
	 * XXX fgets() still receives text in the console's input code page.  This
	 * makes non-ASCII credentials unportable.
	 *
	 * Unintuitively, we also open termin in mode "w+", even though we only
	 * read it; that's needed for SetConsoleMode() to succeed.
	 */
	termin = fopen("CONIN$", "w+");
	termout = fopen("CONOUT$", "w+");
#else

	/*
	 * Do not try to collapse these into one "w+" mode file. Doesn't work on
	 * some platforms (eg, HPUX 10.20).
	 */
	termin = fopen("/dev/tty", "r");
	termout = fopen("/dev/tty", "w");
#endif
	if (!termin || !termout
#ifdef WIN32

	/*
	 * Direct console I/O does not work from the MSYS 1.0.10 console.  Writes
	 * reach nowhere user-visible; reads block indefinitely.  XXX This affects
	 * most Windows terminal environments, including rxvt, mintty, Cygwin
	 * xterm, Cygwin sshd, and PowerShell ISE.  Switch to a more-generic test.
	 */
		|| (getenv("OSTYPE") && strcmp(getenv("OSTYPE"), "msys") == 0)
#endif
		)
	{
		if (termin)
			fclose(termin);
		if (termout)
			fclose(termout);
		termin = stdin;
		termout = stderr;
	}

	if (!echo)
	{
#if defined(HAVE_TERMIOS_H)
		/* disable echo via tcgetattr/tcsetattr */
		tcgetattr(fileno(termin), &t);
		t_orig = t;
		t.c_lflag &= ~ECHO;
		tcsetattr(fileno(termin), TCSAFLUSH, &t);
#elif defined(WIN32)
		/* need the file's HANDLE to turn echo off */
		t = (HANDLE) _get_osfhandle(_fileno(termin));

		/* save the old configuration first */
		GetConsoleMode(t, &t_orig);

		/* set to the new mode */
		SetConsoleMode(t, ENABLE_LINE_INPUT | ENABLE_PROCESSED_INPUT);
#endif
	}

	if (prompt)
	{
		fputs((prompt), termout);
		fflush(termout);
	}

      nread =  getline(&result, &len, termin); 
      
      if (nread == -1) {
    // Handle error or EOF
    if (ferror(termin)) { // Check for an error
        perror("getline"); // Print error message
        // ... error handling ...
    } else if (feof(termin)) { // Check for EOF
        // ... handle EOF ...
    }
}

	//result = getline(termin, prompt_ctx);

	/* If we failed to read anything, just return an empty string */
	if (result == NULL)
		result = apache_strdup("");

	/* strip trailing newline, including \r in case we're on Windows */
	(void) strip_crlf(result);

	if (!echo)
	{
		/* restore previous echo behavior, then echo \n */
#if defined(HAVE_TERMIOS_H)
		tcsetattr(fileno(termin), TCSAFLUSH, &t_orig);
		fputs("\n", termout);
		fflush(termout);
#elif defined(WIN32)
		SetConsoleMode(t, t_orig);
		fputs("\n", termout);
		fflush(termout);
#endif
	}
	else if (prompt_ctx && prompt_ctx->canceled)
	{
		/* also echo \n if prompt was canceled */
		fputs("\n", termout);
		fflush(termout);
	}

	if (termin != stdin)
	{
		fclose(termin);
		fclose(termout);
	}

	return result;
}

/*
 * "Safe" wrapper around apache_strdup().
 */
char *
apache_strdup(const char *in)
{
        char       *tmp;

        if (!in)
        {
                fprintf(stderr,
                                ("cannot duplicate null pointer (internal error)\n"));
                exit(EXIT_FAILURE);
        }
        tmp = strdup(in);
        if (!tmp)
        {
                fprintf(stderr, ("out of memory\n"));
                exit(EXIT_FAILURE);
        }
        return tmp;
}

bool is_version_format(const char *version) {
    int num_count = 0, dot_count = 0;
    
    // Null or empty string check
    if (!version || *version == '\0') {
        return false;
    }
    
    while (*version) {
        if (isdigit(*version)) {
            num_count++;
        } else if (*version == '.') {
            if (num_count == 0) { // Dot should not come before a number
                return false;
            }
            dot_count++;
            num_count = 0; // Reset for the next segment
        } else {
            return false; // Invalid character
        }
        version++;
    }
    
    // Valid format should have exactly two dots and no trailing dots
    return dot_count == 2 && num_count > 0;
}


void printBorder(const char *start, const char *end, const char *color) {
    printf("%s%s", color, start);
    for (int i = 0; i < BOX_WIDTH - 2; i++) {
        printf("─");
    }
    printf("%s%s\n", end, RESET);
}

void printTextBlock(const char *text, const char *textColor, const char *borderColor) {
    const char *start = text;
    const char *end;

    while (*start) {
        end = strchr(start, '\n');
        if (end == NULL) end = start + strlen(start);
        
        int length = end - start;
        int maxLength = BOX_WIDTH - 4;  // Reserve space for null terminator
        if (length > maxLength) length = maxLength;  // Prevent overflow

        char line[BOX_WIDTH - 3];
        strncpy(line, start, length);
        line[length] = '\0';  // Ensure null termination

        printf("%s│%s", borderColor, RESET);
        printf(" %s%-*s%s ", textColor, maxLength, line, RESET);
        printf("%s│%s\n", borderColor, RESET);

        start = (*end == '\n') ? end + 1 : end;
    }
}


const char* component_to_string(Component comp) {
    switch(comp) {
        // Flink
        case FLINK: return "flink";

        // Hadoop
        case HDFS: return "hdfs";
        // HBase
        case HBASE: return "hbase";

        // Hive
        case HIVE: return "hive";
        // Kafka
        case KAFKA: return "kafka";

        // Livy
        case LIVY: return "livy";

        // Phoenix
        case PHOENIX: return "phoenix";

        // Ranger
        case RANGER: return "ranger";

        // Solr
        case SOLR: return "solr";
        // Tez
        case TEZ: return "tez";
        case ATLAS: return "atlas";
        case STORM: return "storm";
        case PIG: return "pig";
        case SPARK: return "spark";
        case PRESTO: return "presto";

        // Zeppelin
        case ZEPPELIN: return "zeppelin";

        // Zookeeper
        case ZOOKEEPER: return "zookeeper";
        default: return NULL;
    }
}

Component string_to_component(const char* name) {
    // Flink
    if (strcmp(name, "flink") == 0) return FLINK;
    if (strcmp(name, "hdfs") == 0) return HDFS;
 
    // HBase
    if (strcmp(name, "hbase") == 0) return HBASE;
    // Hive
    if (strcmp(name, "hive") == 0) return HIVE;

    // Kafka
    if (strcmp(name, "kafka") == 0) return KAFKA;

    // Livy
    if (strcmp(name, "livy") == 0) return LIVY;

    // Phoenix
    if (strcmp(name, "phoenix") == 0) return PHOENIX;

    // Ranger
    if (strcmp(name, "ranger") == 0) return RANGER;

    // Solr
    if (strcmp(name, "solr") == 0) return SOLR;
    // Tez
    if (strcmp(name, "tez") == 0) return TEZ;

    // Zeppelin
    if (strcmp(name, "zeppelin") == 0) return ZEPPELIN;

    // Zookeeper
    if (strcmp(name, "zookeeper") == 0) return ZOOKEEPER;
    return NONE;
}

const char * action_to_string(Action action) {
    switch(action) {
        case START:
            return "Starting...";
        case STOP:
            return "Stoping...";
        case RESTART: 
            return "Restarting...";
        case INSTALL: 
            return "Installing...";
        case UPGRADE:
            return "Upgrading...";
        case UNINSTALL: 
            return "Uninstalling...";
        case CONFIGURE:
            return "Configuring...";  
        /* Add cases for all components */
        default: fprintf(stderr, "Unknown component\n"); break;
    }
    return NULL;
}


// Helper function to trim leading whitespace
char* trim_leading(char* str) {
    while (isspace((unsigned char)*str)) str++;
    return str;
}

int update_component_version(const char* component, const char* new_version) {
    FILE* file = fopen("bigtop-master/bigtop.bom", "r");
    if (!file) {
        perror("Error opening file");
        return -1;
    }

    // Read all lines into memory
    char** lines = NULL;
    size_t line_count = 0;
    char buffer[MAX_LINE_LENGTH];
    while (fgets(buffer, MAX_LINE_LENGTH, file)) {
        char* line = strdup(buffer);
        if (!line) {
            fclose(file);
            return -1;
        }
        lines = realloc(lines, sizeof(char*) * (line_count + 1));
        if (!lines) {
            free(line);
            fclose(file);
            return -1;
        }
        lines[line_count++] = line;
    }
    fclose(file);

    int component_found = 0;
    int component_braces = 0;
    int version_braces = 0;
    int result = -1; // Default to error

    char component_pattern[MAX_LINE_LENGTH];
    snprintf(component_pattern, sizeof(component_pattern), "'%s' {", component);

    for (size_t i = 0; i < line_count; i++) {
        if (!component_found) {
            char* trimmed = trim_leading(lines[i]);
            if (strncmp(trimmed, component_pattern, strlen(component_pattern)) == 0) {
                component_found = 1;
                component_braces = 1;
                // Check the rest of the line for braces
                for (char* c = lines[i]; *c; c++) {
                    if (*c == '{') component_braces++;
                    else if (*c == '}') component_braces--;
                }
                continue;
            }
        } else {
            // Track component braces
            for (char* c = lines[i]; *c; c++) {
                if (*c == '{') component_braces++;
                else if (*c == '}') component_braces--;
            }

            // Check if we are outside the component block
            if (component_braces <= 0) {
                component_found = 0;
                continue;
            }

            // Check if entering version block
            char* trimmed = trim_leading(lines[i]);
            if (strncmp(trimmed, "version {", 9) == 0) {
                version_braces = 1;
                // Check the rest of the line for braces
                for (char* c = lines[i]; *c; c++) {
                    if (*c == '{') version_braces++;
                    else if (*c == '}') version_braces--;
                }

                // Search for base line within version block
                for (size_t j = i; j < line_count; j++) {
                    // Update version_braces for current line
                    if (j > i) {
                        version_braces = 1;
                        for (char* c = lines[j]; *c; c++) {
                            if (*c == '{') version_braces++;
                            else if (*c == '}') version_braces--;
                        }
                    }

                    // Look for base line
                    char* base_ptr = strstr(lines[j], "base =");
                    if (base_ptr) {
                        char* start = strchr(base_ptr, '\'');
                        if (!start) continue;
                        start++;
                        char* end = strchr(start, '\'');
                        if (!end) continue;

                        // Replace the version
                        size_t prefix_len = start - lines[j];
                        char* new_line = malloc(prefix_len + strlen(new_version) + (strlen(end + 1) + 2));
                        if (!new_line) {
                            result = -1;
                            goto cleanup;
                        }
                        snprintf(new_line, prefix_len + strlen(new_version) + strlen(end + 1) + 2, 
                                 "%.*s'%s'%s", (int)prefix_len, lines[j], new_version, end + 1);
                        free(lines[j]);
                        lines[j] = new_line;
                       // base_line = j;
                        result = 0;
                        goto write_back;
                    }

                    // Exit version block if braces are closed
                    if (version_braces <= 0) {
                        break;
                    }
                }
            }
        }
    }

write_back:
    if (result == 0) {
        file = fopen("bigtop-master/bigtop.bom", "w");
        if (!file) {
            perror("Error opening file for writing");
            result = -1;
            goto cleanup;
        }
        for (size_t i = 0; i < line_count; i++) {
            fputs(lines[i], file);
        }
        fclose(file);
    }

cleanup:
    for (size_t i = 0; i < line_count; i++) {
        free(lines[i]);
    }
    free(lines);

    return result;
}

// Example usage:
// update_component_version("BillOfMaterial.txt", "kafka", "3.5.0");



bool executeSystemCommand(const char *cmd) {
    int ret = system(cmd);

#ifdef _WIN32
    /* Windows-specific return value handling */
    if (ret >= 0) {
        return true;
    } else {
        fprintf(stderr, "System command execution failed with error code: %d\n", ret);
        return false;
    }
#else
    /* POSIX-compliant return value handling */
    if (ret == -1) {
        perror("system() execution failed");
        return false;
    } else 
        return true;
#endif
}



static const char* hadoop_versions[] = {"2.10.2", "3.2.4", "3.3.5", "3.3.6", "3.4.0", "3.4.1", NULL};
static const char* presto_versions[] = {NULL};
static const char* pig_versions[] = {"0.16.0", "0.17.0", NULL};
static const char* hbase_versions[] = {"2.4.18", "2.5.11", "2.6.1", "2.6.2", NULL};
static const char* hive_versions[] = {"4.0.1", NULL};
static const char* flink_versions[] = {"1.17.2", "1.18.1", "1.19.0", "1.19.1", "1.19.2", "1.20.0", "1.20.1", NULL};
static const char* livy_versions[] = {"0.7.1", NULL};
static const char* tez_versions[] = {"0.10.1", "0.10.2", "0.10.3", "0.10.4", "0.9.2", NULL};
static const char* ranger_versions[] = {"0.6.3", "0.7.1", "1.0.0", "1.1.0", "1.2.0", "2.0.0", "2.1.0", "2.2.0", "2.3.0", "2.4.0", "2.5.0", "2.6.0", NULL};
static const char* phoenix_versions[] = {"4.16.1", "5.1.2", "5.1.3", "5.2.0", "5.2.1", NULL};
static const char* solr_versions[] = {"9.8.1", NULL};
static const char* spark_versions[] = {"3.4.4", "3.5.5", NULL};
static const char* zeppelin_versions[] = {"0.10.0", "0.10.1", "0.11.0", "0.11.1", "0.11.2", "0.12.0", "0.8.2", "0.9.0", NULL};
static const char* kafka_versions[] = {"3.7.2", "3.8.0", "3.8.1", "3.9.0", NULL};
static const char* zookeeper_versions[] = {"3.7.2", "3.8.4", "3.9.3", NULL};

bool isComponentVersionSupported(Component component, const char *version) {
    const char **supported_versions = NULL;

    switch (component) {
        case HDFS:
            supported_versions = hadoop_versions;
            break;
        case PRESTO:
            supported_versions = presto_versions;
            break;
        case PIG:
            supported_versions = pig_versions;
            break;
        case HBASE:
            supported_versions = hbase_versions;
            break;
        case HIVE:
            supported_versions = hive_versions;
            break;
        case FLINK:
            supported_versions = flink_versions;
            break;
        case LIVY:
            supported_versions = livy_versions;
            break;
        case TEZ:
            supported_versions = tez_versions;
            break;
        case RANGER:
            supported_versions = ranger_versions;
            break;
        case PHOENIX:
            supported_versions = phoenix_versions;
            break;
        case SOLR:
            supported_versions = solr_versions;
            break;
        case SPARK:
            supported_versions = spark_versions;
            break;
        case ZEPPELIN:
            supported_versions = zeppelin_versions;
            break;
        case KAFKA:
            supported_versions = kafka_versions;
            break;
        case ZOOKEEPER:
            supported_versions = zookeeper_versions;
            break;
        default:
            return false;
    }

    if (version == NULL) {
        return false;
    }

    for (int i = 0; supported_versions != NULL && supported_versions[i] != NULL; i++) {
        if (strcmp(version, supported_versions[i]) == 0) {
            return true;
        }
    }

    return false;
}

void buffer_intermediary_function(ClientSocket *client_sock, char *buf) {
    if (buf == NULL || *buf == '\0') {
        return; // Handle NULL or empty input
    }

    size_t start = 0;
    size_t buf_len = strlen(buf);

    while (start < buf_len) {
        // Find next newline starting from current position
        char *newline = strchr(buf + start, '\n');
        
        if (newline != NULL) {
            size_t end = newline - buf + 1; // Include the newline
            internal_flush_buffer(client_sock, buf, &start, &end);
            
            // Advance start position based on flush result
            if (start < end) {
                start = end;
            } else {
                break; // Handle potential flush error
            }
        } else {
            // No more newlines - flush remaining content
            size_t end = buf_len;
            internal_flush_buffer(client_sock, buf, &start, &end);
            break;
        }
    }
}


int FPRINTF(ClientSocket *client_sock, const char *format, ...) {
    va_list args;
    int needed;
    char *buffer = NULL;
    int                     rc;
    va_start(args, format);
    needed = vsnprintf(NULL, 0, format, args);
    va_end(args);

    if (needed < 0) {
        return -1;
    }

    buffer = malloc(needed + 1);
    if (!buffer) {
        return -1;
    }

    va_start(args, format);
    vsnprintf(buffer, needed + 1, format, args);
    va_end(args);

        /* We'll retry after EINTR, but ignore all other failures */
        do
        {
                rc = send(client_sock->sock, buffer, strlen(buffer) + 1, 0);
        } while (rc < 0 && errno == EINTR);
   // buffer_intermediary_function(client_sock, buffer);
    free(buffer);

    return needed;
}

int PRINTF(ClientSocket *client_sock, const char *format, ...) {
    va_list args;
    int needed;
    char *buffer = NULL;
    int                     rc;

    va_start(args, format);
    needed = vsnprintf(NULL, 0, format, args);
    va_end(args);

    if (needed < 0) {
        return -1;
    }

    buffer = malloc(needed + 1);
    if (!buffer) {
        return -1;
    }

    va_start(args, format);
    vsnprintf(buffer, needed + 1, format, args);
    va_end(args);


        /* We'll retry after EINTR, but ignore all other failures */
        do
        {
                rc = send(client_sock->sock, buffer, strlen(buffer) + 1, 0);
        } while (rc < 0 && errno == EINTR);
   // buffer_intermediary_function(client_sock, buffer);
    free(buffer);

    return needed;
}

void PERROR(ClientSocket *client_sock, const char *s) {
    int errno_copy = errno; // Save errno as subsequent calls may alter it
    const char *error_str = strerror(errno_copy);
    int needed;
    char *buffer = NULL;
    int                     rc;

    if (s == NULL) {
        s = "";
    }

    if (*s) {
        needed = snprintf(NULL, 0, "%s: %s\n", s, error_str);
    } else {
        needed = snprintf(NULL, 0, "%s\n", error_str);
    }

    if (needed < 0) {
        return;
    }

    buffer = malloc(needed + 1);
    if (!buffer) {
        return;
    }

    if (*s) {
        snprintf(buffer, needed + 1, "%s: %s\n", s, error_str);
    } else {
        snprintf(buffer, needed + 1, "%s\n", error_str);
    }
        /* We'll retry after EINTR, but ignore all other failures */
        do
        {
                rc = send(client_sock->sock, buffer, strlen(buffer) + 1, 0);
        } while (rc < 0 && errno == EINTR);
        
    //buffer_intermediary_function(client_sock, buffer);
    free(buffer);
}

char **split_string(char *input) {
    if (input == NULL) {
        return NULL;
    }

    char *delim = strchr(input, ','); // Find the first comma
    char **result = (char **)malloc(2 * sizeof(char *));
    if (result == NULL) {
        return NULL;
    }

    if (delim == NULL) {
        // No comma: Assume second string is NULL (as per concatenate logic)
        result[0] = strdup(input);
        result[1] = NULL;
        // Check for strdup failure
        if (result[0] == NULL) {
            free(result);
            return NULL;
        }
    } else {
        // Split into two parts
        size_t first_len = delim - input;
        result[0] = (char *)malloc(first_len + 1);
        if (result[0] == NULL) {
            free(result);
            return NULL;
        }
        memcpy(result[0], input, first_len);
        result[0][first_len] = '\0';

        char *second_part = delim + 1; // Skip the comma
        result[1] = strdup(second_part);
        if (result[1] == NULL) {
            free(result[0]);
            free(result);
            return NULL;
        }
    }

    return result;
}

void handle_result(ConfigStatus status) {
    switch(status) {
        case SUCCESS:
            PRINTF(global_client_socket,  "Operation completed successfully\n");
            break;
            
        case INVALID_FILE_TYPE:
            FPRINTF(global_client_socket, "Error: Invalid file type provided\n");
            break;
            
        case FILE_NOT_FOUND:
            FPRINTF(global_client_socket, "Error: Configuration file not found\n");
            break;
            
        case XML_PARSE_ERROR:
            FPRINTF(global_client_socket, "Error: Failed to parse XML content\n");
            break;
            
        case INVALID_CONFIG_FILE:
            FPRINTF(global_client_socket, "Error: Invalid configuration file format\n");
            break;
            
        case XML_UPDATE_ERROR:
            FPRINTF(global_client_socket, "Error: Failed to update XML configuration\n");
            break;
            
        case FILE_WRITE_ERROR:
            FPRINTF(global_client_socket, "Error: Could not write to file\n");
            break;
            
        case FILE_READ_ERROR:
            FPRINTF(global_client_socket, "Error: Could not read from file\n");
            break;
            
        case XML_INVALID_ROOT:
            FPRINTF(global_client_socket, "Error: XML root element is invalid or missing\n");
            break;
            
        case SAVE_FAILED:
            FPRINTF(global_client_socket, "Error: Failed to save configuration changes\n");
            break;
            
        default:
            FPRINTF(global_client_socket, "Unknown error occurred\n");
            break;
    }
}

bool handleValidationResult(ValidationResult result) {
    switch (result) {
        case VALIDATION_OK:
            return true;
            
        case ERROR_PARAM_NOT_FOUND:
            FPRINTF(global_client_socket, "Error: Parameter not found in  configuration\n");
            break;
            
        case ERROR_VALUE_EMPTY:
            FPRINTF(global_client_socket, "Error: Configuration value cannot be empty\n");
            break;
            
        case ERROR_INVALID_FORMAT:
            FPRINTF(global_client_socket, "Error: Invalid format for configuration parameter\n");
            break;
            
        case ERROR_CONSTRAINT_VIOLATED:
            FPRINTF(global_client_socket, "Error: Parameter value violates constraints\n");
            break;
            
        default:
            FPRINTF(global_client_socket, "Error: Unknown validation error\n");
            break;
    }
    return false;
}


int set_config(const char *filename, const char *key, const char *value) {
    FILE *file = fopen(filename, "r");
    char **lines = NULL;
    size_t line_count = 0;
    char *buffer = NULL;
    size_t buffer_size = 0;
    ssize_t read;
    int found = 0;
    int update_occurred = 0;

    // Read existing file or handle non-existent file
    if (file) {
        while ((read = getline(&buffer, &buffer_size, file)) != -1) {
            // Allocate and store each line
            char *line = strdup(buffer);
            if (!line) goto cleanup_error;
            
            char **new_lines = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!new_lines) { free(line); goto cleanup_error; }
            
            lines = new_lines;
            lines[line_count++] = line;
        }
        free(buffer);
        fclose(file);
    } else if (errno != ENOENT) {
        return -1;  // Return error if non-ENOENT failure
    }

    // Process each line to find and update key
    for (size_t i = 0; i < line_count; i++) {
        char *line = lines[i];
        char *p = line;
        
        // Skip leading whitespace
        while (isspace((unsigned char)*p)) p++;
        
        // Skip comments and empty lines
        if (*p == '#' || *p == ';' || *p == '\0') continue;
        
        // Locate key and validate format
        char *equals = strchr(p, '=');
        if (!equals) continue;
        
        // Extract key segment
        char *key_end = equals;
        while (key_end > p && isspace((unsigned char)*(key_end - 1))) key_end--;
        
        // Compare keys
        size_t key_len = key_end - p;
        if (key_len == strlen(key) && strncmp(p, key, key_len) == 0) {
            // Update the line with new key-value pair
            char *new_line;
            if (asprintf(&new_line, "%s=%s\n", key, value) < 0) goto cleanup_error;
            
            free(lines[i]);
            lines[i] = new_line;
            found = update_occurred = 1;
            break;  // Update first occurrence only
        }
    }

    // Append key-value if not found
    if (!found) {
        char *new_line;
        if (asprintf(&new_line, "%s=%s\n", key, value) < 0) goto cleanup_error;
        
        char **new_lines = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!new_lines) { free(new_line); goto cleanup_error; }
        
        lines = new_lines;
        lines[line_count++] = new_line;
        update_occurred = 1;
    }

    // Write changes back to file if updates occurred
    if (update_occurred) {
        file = fopen(filename, "w");
        if (!file) goto cleanup_error;
        
        for (size_t i = 0; i < line_count; i++) {
            fputs(lines[i], file);
        }
        fclose(file);
    }

    // Cleanup memory
    for (size_t i = 0; i < line_count; i++) free(lines[i]);
    free(lines);
    return 0;

cleanup_error:
    // Error handling: free resources
    if (buffer) free(buffer);
    if (file) fclose(file);
    for (size_t i = 0; i < line_count; i++) free(lines[i]);
    free(lines);
    return -1;
}


// Helper function to check if a line contains the target key
static bool is_key_present(const char *line, const char *key) {
    // Skip leading whitespace
    while (*line == ' ' || *line == '\t' || *line == '\f') {
        line++;
    }
    
    // Skip comment lines
    if (*line == '#' || *line == '!') {
        return false;
    }
    
    // Check for key match
    const char *k = key;
    while (*k != '\0') {
        if (*line != *k) {
            return false;
        }
        line++;
        k++;
    }
    
    // Verify separator after key
    return (*line == '=' || *line == ':' || *line == ' ' || 
            *line == '\t' || *line == '\f' || *line == '\0' || 
            *line == '\r' || *line == '\n');
}

// Main configuration function
int configure_hadoop_property(const char *file_path, const char *key, const char *value) {
    FILE *fp = fopen(file_path, "r");
    if (!fp) {
        perror("❌ Error opening file");
        return -1;
    }

    // Read all lines into memory
    char **lines = NULL;
    size_t line_count = 0;
    char buffer[1024];
    bool key_found = false;
    int status = 0; // 0=success, -1=error

    while (fgets(buffer, sizeof(buffer), fp)) {
        // Allocate space for new line pointer
        char **new_lines = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!new_lines) {
            status = -1;
            goto CLEANUP_READ;
        }
        lines = new_lines;

        // Check for key presence and update if found
        if (!key_found && is_key_present(buffer, key)) {
            lines[line_count] = malloc(strlen(key) + strlen(value) + 3);
            if (!lines[line_count]) {
                status = -1;
                goto CLEANUP_READ;
            }
            sprintf(lines[line_count], "%s=%s\n", key, value);
            key_found = true;
        } 
        else {
            // Preserve existing line
            lines[line_count] = strdup(buffer);
            if (!lines[line_count]) {
                status = -1;
                goto CLEANUP_READ;
            }
        }
        line_count++;
    }

    if (ferror(fp)) {
        perror("❌ Error reading file");
        status = -1;
        goto CLEANUP_READ;
    }
    fclose(fp);
    fp = NULL;

    // Append new key-value pair if not found
    if (!key_found) {
        char **new_lines = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!new_lines) {
            status = -1;
            goto CLEANUP_READ;
        }
        lines = new_lines;
        
        lines[line_count] = malloc(strlen(key) + strlen(value) + 3);
        if (!lines[line_count]) {
            status = -1;
            goto CLEANUP_READ;
        }
        sprintf(lines[line_count], "%s=%s\n", key, value);
        line_count++;
    }

    // Write updated content back to file
    fp = fopen(file_path, "w");
    if (!fp) {
        perror("❌ Error opening file for writing");
        status = -1;
        goto CLEANUP_READ;
    }

    for (size_t i = 0; i < line_count; i++) {
        if (fputs(lines[i], fp) == EOF) {
            perror("❌ Error writing to file");
            status = -1;
            break;
        }
    }

    // Cleanup resources
CLEANUP_READ:
    if (fp) fclose(fp);
    if (lines) {
        for (size_t i = 0; i < line_count; i++) {
            if (lines[i]) free(lines[i]);
        }
        free(lines);
    }
    return status;
}

int updateHadoopConfigXML(const char *filePath, const char *parameterName, const char *parameterValue) {
    // Initialize libxml2
    xmlInitParser();
    LIBXML_TEST_VERSION

    // Check file accessibility
    FILE *file = fopen(filePath, "r");
    if (!file) {
        switch (errno) {
            case ENOENT: return 1;  // File not found
            case EACCES: return 2;  // Permission denied
            default: return 3;      // Other errors
        }
    }
    fclose(file);

    xmlDoc *doc = NULL;
    xmlNode *root = NULL;
    int retCode = 0;
    int found = 0;

    // Parse XML document
    doc = xmlReadFile(filePath, NULL, 0);
    if (!doc) {
        xmlErrorPtr err = xmlGetLastError();
        if (err && err->code == XML_ERR_DOCUMENT_EMPTY) {
            // Create new document for empty file
            doc = xmlNewDoc(BAD_CAST "1.0");
            if (!doc) {
                xmlCleanupParser();
                return 4;  // Memory allocation failure
            }
            root = xmlNewNode(NULL, BAD_CAST "configuration");
            if (!root) {
                xmlFreeDoc(doc);
                xmlCleanupParser();
                return 4;
            }
            xmlDocSetRootElement(doc, root);
        } else {
            xmlCleanupParser();
            return 5;  // XML parse error
        }
    }

    // Get root node (create if missing)
    root = xmlDocGetRootElement(doc);
    if (!root) {
        root = xmlNewNode(NULL, BAD_CAST "configuration");
        if (!root) {
            xmlFreeDoc(doc);
            xmlCleanupParser();
            return 4;
        }
        xmlDocSetRootElement(doc, root);
    }

    // Validate root node name
    if (xmlStrcmp(root->name, BAD_CAST "configuration")) {
        xmlFreeDoc(doc);
        xmlCleanupParser();
        return 6;  // Invalid root node
    }

    // Search and update existing properties
    for (xmlNode *prop = root->children; prop; prop = prop->next) {
        if (prop->type != XML_ELEMENT_NODE || xmlStrcmp(prop->name, BAD_CAST "property"))
            continue;

        xmlNode *nameNode = NULL;
        xmlNode *valueNode = NULL;
        
        // Find name and value nodes
        for (xmlNode *child = prop->children; child; child = child->next) {
            if (child->type != XML_ELEMENT_NODE) continue;
            if (!xmlStrcmp(child->name, BAD_CAST "name")) 
                nameNode = child;
            else if (!xmlStrcmp(child->name, BAD_CAST "value")) 
                valueNode = child;
        }

        // Check parameter name match
        xmlChar *nameContent = nameNode ? xmlNodeGetContent(nameNode) : NULL;
        if (nameContent && !xmlStrcmp(nameContent, BAD_CAST parameterName)) {
            found = 1;
            if (valueNode) {
                // Update existing value
                xmlNodeSetContent(valueNode, BAD_CAST parameterValue);
            } else {
                // Add missing value node
                valueNode = xmlNewTextChild(prop, NULL, BAD_CAST "value", BAD_CAST parameterValue);
                if (!valueNode) retCode = 4;
            }
        }
        if (nameContent) xmlFree(nameContent);
    }

    // Add new property if not found
    if (!found && !retCode) {
        xmlNode *newProp = xmlNewNode(NULL, BAD_CAST "property");
        if (!newProp) {
            retCode = 4;
        } else {
            xmlNode *nameNode = xmlNewTextChild(newProp, NULL, BAD_CAST "name", BAD_CAST parameterName);
            xmlNode *valueNode = xmlNewTextChild(newProp, NULL, BAD_CAST "value", BAD_CAST parameterValue);
            
            if (!nameNode || !valueNode) {
                xmlFreeNode(newProp);
                retCode = 4;
            } else {
                xmlAddChild(root, newProp);
            }
        }
    }

    // Save changes if no errors
    if (!retCode) {
        if (xmlSaveFormatFile(filePath, doc, 1) < 0)
            retCode = 7;  // File save error
    }

    // Cleanup resources
    xmlFreeDoc(doc);
    xmlCleanupParser();
    return retCode;
}


/* Helper functions to detect OS */
int is_redhat() {
    struct stat st;
    return (stat("/etc/redhat-release", &st) == 0);
}

int is_debian() {
    struct stat st;
    return (stat("/etc/debian_version", &st) == 0);
}

void trim_whitespace(char *str) {
    char *end;

    // Trim leading space
    while (*str == ' ' || *str == '\t') {
        str++;
    }

    if (*str == 0) {
        return;
    }

    // Trim trailing space
    end = str + strlen(str) - 1;
    while (end > str && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) {
        end--;
    }
    *(end + 1) = 0;
}

char *trim(char *str) {
    char *end;

    // Trim leading spaces
    while (isspace((unsigned char)*str)) str++;

    // Trim trailing spaces
    if (*str == 0) return str; // All spaces?

    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;

    // Null-terminate the string
    *(end + 1) = '\0';

    return str;
  }

int mkdir_p(const char *path) {
    char tmp[MAX_PATH_LEN];
    char *p = NULL;
    size_t len;

    strncpy(tmp, path, sizeof(tmp));
    tmp[sizeof(tmp) - 1] = '\0';
    len = strlen(tmp);
    if (len == 0) return -1;
    if (tmp[len - 1] == '/') {
        tmp[len - 1] = '\0';
    }

    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, S_IRWXU) != 0 && errno != EEXIST) {
                return -1;
            }
            *p = '/';
        }
    }
    if (mkdir(tmp, S_IRWXU) != 0 && errno != EEXIST) {
        return -1;
    }
    return 0;
}

char* generate_regex_pattern(const char* canonical_name) {
    char* copy = strdup(canonical_name);
    if (!copy) return NULL;

    int num_parts = 1;
    for (char* p = copy; *p; p++) {
        if (*p == '.') num_parts++;
    }

    char** parts = malloc(num_parts * sizeof(char*));
    if (!parts) {
        free(copy);
        return NULL;
    }

    int i = 0;
    char* token = strtok(copy, ".");
    while (token != NULL) {
        parts[i++] = token;
        token = strtok(NULL, ".");
    }

    size_t regex_len = 2; // For ^ and $
    regex_len += (i - 1) * strlen("[._-]+");
    for (int j = 0; j < i; j++) {
        regex_len += strlen(parts[j]);
    }

    // Check for potential overflow when adding 1 to regex_len
    if (regex_len > SIZE_MAX - 1) {
        fprintf(stderr, "Error: regex_len is too large\n");
        free(copy);
        free(parts);
        return NULL;
    }

    size_t needed = regex_len + 1;
    // Check if the needed size exceeds system's maximum allowed allocation size
    if (needed > (size_t)SSIZE_MAX) {
        fprintf(stderr, "Error: regex pattern exceeds maximum allowed size\n");
        free(copy);
        free(parts);
        return NULL;
    }

    char* regex_pattern = malloc(needed);
    if (!regex_pattern) {
        free(copy);
        free(parts);
        return NULL;
    }
    regex_pattern[0] = '\0';

    strcat(regex_pattern, "^");
    for (int j = 0; j < i; j++) {
        strcat(regex_pattern, parts[j]);
        if (j < i - 1) {
            strcat(regex_pattern, "[._-]+");
        }
    }
    strcat(regex_pattern, "$");

    free(copy);
    free(parts);
    return regex_pattern;
}

bool isPositiveInteger(const char *value) {
    char *end;
    long num = strtol(value, &end, 10);
    return *end == '\0' && num > 0;
}

bool isValidPort(const char *value) {
    char *end;
    long port = strtol(value, &end, 10);
    return *end == '\0' && port > 0 && port <= 65535;
}

bool isValidBoolean(const char *value) {
    return strcmp(value, "true") == 0 || strcmp(value, "false") == 0;
}

bool isValidHostPort(const char *value) {
    char *colon = strchr(value, ':');
    if (!colon) return false;
    return isValidPort(colon + 1);
}

bool isDataSize(const char *value) {
    char *end;
    strtol(value, &end, 10);
    if (*end == '\0') return true;
    
    if (end == value) return false;
    return tolower(*end) == 'k' || tolower(*end) == 'm' || 
           tolower(*end) == 'g' || tolower(*end) == 't';
}

bool isValidDuration(const char *value) {
    char *end;
    long num = strtol(value, &end, 10);
    if (num <= 0) return false;
    if (*end == '\0') return true; // Assume seconds if no unit
    if (strlen(end) != 1) return false;
    char unit = tolower(*end);
    return (unit == 's' || unit == 'm' || unit == 'h' || unit == 'd');
}

bool isValidCommaSeparatedList(const char *value) {
    if (strlen(value) == 0) return false;
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    while (token != NULL) {
        if (strlen(token) == 0) {
            free(copy);
            return false;
        }
        token = strtok(NULL, ",");
    }
    free(copy);
    return true;
}

bool isNonNegativeInteger(const char *value) {
    char *end;
    long num = strtol(value, &end, 10);
    return *end == '\0' && num >= 0;
}

// New helper function for comma-separated host:port lists
bool isValidHostPortList(const char *value) {
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    bool valid = true;
    
    while (token != NULL) {
        char *colon = strchr(token, ':');
        if (!colon) {
            valid = false;
            break;
        }
        if (!isValidPort(colon + 1)) {
            valid = false;
            break;
        }
        token = strtok(NULL, ",");
    }
    
    free(copy);
    return valid;
}

// New helper for URL validation
bool isValidUrl(const char *value) {
    return strncmp(value, "http://", 7) == 0 || 
           strncmp(value, "https://", 8) == 0 ||
           strncmp(value, "jceks://", 8) == 0;
}

// New helper for directory/file paths
bool isValidPath(const char *value) {
    return strlen(value) > 0 && value[0] != '\0';
}



// HBase-specific helper functions
bool isValidHBaseDuration(const char *value) {
    char *end;
    long num = strtol(value, &end, 10);
    if (num <= 0) return false;
    if (*end == '\0') return true; // Assume milliseconds
    if (strlen(end) > 2) return false;
    
    // Allow ms/s/min/h/d suffixes
    return strcmp(end, "ms") == 0 || strcmp(end, "s") == 0 ||
           strcmp(end, "min") == 0 || strcmp(end, "h") == 0 ||
           strcmp(end, "d") == 0;
}

bool isValidPrincipalFormat(const char *value) {
    return strchr(value, '/') != NULL && strchr(value, '@') != NULL;
}

bool isDataSizeWithUnit(const char *value) {
    char *end;
    strtol(value, &end, 10);
    if (*end == '\0') return true; // No unit = bytes
    if (end == value) return false;
    return tolower(*end) == 'k' || tolower(*end) == 'm' || 
           tolower(*end) == 'g' || tolower(*end) == 't';
}

bool isValidEncoding(const char *value) {
    // Supported encodings - extend as needed
    const char *valid_encodings[] = {"UTF-8", "UTF-16", "ISO-8859-1", "ASCII"};
    for (size_t i = 0; i < sizeof(valid_encodings)/sizeof(valid_encodings[0]); i++) {
        if (strcmp(value, valid_encodings[i]) == 0) return true;
    }
    return false;
}

bool isValidSparkDuration(const char *value) {
    char *end;
    long num = strtol(value, &end, 10);
    if (num <= 0) return false;
    if (*end == '\0') return true; // Assume seconds
    if (strlen(end) > 2) return false;
    
    // Allow s/min/h/d suffixes
    return strcmp(end, "s") == 0 || strcmp(end, "min") == 0 || 
           strcmp(end, "h") == 0 || strcmp(end, "d") == 0 ||
           strcmp(end, "ms") == 0;
}

bool isValidSparkMasterFormat(const char *value) {
    return strncmp(value, "local", 5) == 0 ||
           strncmp(value, "yarn", 4) == 0 ||
           strncmp(value, "spark://", 8) == 0 ||
           strncmp(value, "k8s://", 6) == 0;
}


// Kafka-specific helper functions
bool isHostPortPair(const char *value) {
    char *colon = strchr(value, ':');
    return colon && isValidPort(colon + 1);
}

bool isValidCompressionType(const char *value) {
    const char *valid[] = {"none", "gzip", "snappy", "lz4", "zstd", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

bool isSecurityProtocolValid(const char *value) {
    const char *valid[] = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

bool isSaslMechanismValid(const char *value) {
    const char *valid[] = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

bool isAutoOffsetResetValid(const char *value) {
    const char *valid[] = {"earliest", "latest", "none", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

// Helper function to validate credential file format
bool isValidCredentialFile(const char *value) {
    return strncmp(value, "jceks://file/", 13) == 0;
}


// Flink-specific helper functions
bool isMemorySize(const char *value) {
    char *end;
    strtol(value, &end, 10);
    if (*end == '\0') return true; // bytes
    if (end == value) return false;
    return tolower(*end) == 'k' || tolower(*end) == 'm' || 
           tolower(*end) == 'g' || tolower(*end) == 't';
}

bool isValidURI(const char *value) {
    return strstr(value, "://") != NULL && strlen(value) > 5;
}

bool isFraction(const char *value) {
    char *end;
    float num = strtof(value, &end);
    return *end == '\0' && num >= 0.0f && num <= 1.0f;
}


// ZooKeeper-specific helper functions
bool isTimeMillis(const char *value) {
    char *end;
    long ms = strtol(value, &end, 10);
    if (*end == '\0') return ms > 0;
    if (strcmp(end, "ms") == 0) return ms > 0;
    return false;
}

bool isSizeWithUnit(const char *value) {
    char *end;
    strtol(value, &end, 10);
    if (*end == '\0') return true;
    return end[0] == 'K' || end[0] == 'M' || end[0] == 'G';
}

bool isCommaSeparatedList(const char *value) {
    if (strlen(value) == 0) return false;
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    while (token != NULL) {
        if (strlen(token) == 0) {
            free(copy);
            return false;
        }
        token = strtok(NULL, ",");
    }
    free(copy);
    return true;
}

// Storm-specific helper functions
bool isMemorySizeMB(const char *value) {
    char *end;
     (void)strtol(value, &end, 10);
    if (*end == '\0') return true;
    if (strcasecmp(end, "mb") == 0) return true;
    return false;
}

bool isTimeSeconds(const char *value) {
    char *end;
    long seconds = strtol(value, &end, 10);
    return *end == '\0' && seconds > 0;
}

bool isPercentage(const char *value) {
    char *end;
    float pct = strtof(value, &end);
    return *end == '\0' && pct >= 0.0f && pct <= 100.0f;
}

bool isJDBCURL(const char *value) {
    return strstr(value, "jdbc:") != NULL && strlen(value) > 10;
}

bool isValidCompressionCodec(const char *value) {
    const char *valid[] = {"NONE", "SNAPPY", "GZIP", "LZO", "ZSTD", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}


bool isURL(const char *value) {
    return strstr(value, "://") != NULL && strlen(value) > 8;
}

bool isJCEKSPath(const char *value) {
    return strstr(value, "jceks://file/") == value;
}

bool isValidAuthType(const char *value) {
    const char *valid[] = {"kerberos", "ldap", "jwt", "basic", "none", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

bool isValidSparkMaster(const char *value) {
    return strncmp(value, "local", 5) == 0 ||
           strncmp(value, "yarn", 4) == 0 ||
           strncmp(value, "k8s://", 5) == 0 ||
           strstr(value, "spark://") != NULL;
}

// Solr-specific helper functions
bool isValidZKHostList(const char *value) {
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    bool valid = true;
    
    while (token != NULL) {
        char *colon = strchr(token, ':');
        if (!colon || !isValidPort(colon + 1)) {
            valid = false;
            break;
        }
        token = strtok(NULL, ",");
    }
    
    free(copy);
    return valid;
}

bool isValidLogLevel(const char *value) {
    const char *levels[] = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", NULL};
    for (int i = 0; levels[i]; i++) {
        if (strcmp(value, levels[i]) == 0) return true;
    }
    return false;
}

bool isValidContextPath(const char *value) {
    return value[0] == '/' && strchr(value, ' ') == NULL && 
           strchr(value, ';') == NULL && strchr(value, '\\') == NULL;
}

bool isZKQuorum(const char *value) {
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    bool valid = true;
    
    while (token != NULL) {
        char *colon = strchr(token, ':');
        if (!colon || !isValidPort(colon + 1)) {
            valid = false;
            break;
        }
        token = strtok(NULL, ",");
    }
    
    free(copy);
    return valid;
}

// Ranger-specific helper functions
bool isSSLProtocolValid(const char *value) {
    const char *valid[] = {"TLSv1.2", "TLSv1.3", NULL};
    for (int i = 0; valid[i]; i++)
        if (strstr(value, valid[i]) != NULL) return true;
    return false;
}

bool isComponentValid(const char *component) {
    const char *valid[] = {"hdfs", "hive", "hbase", "kafka", "yarn", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(component, valid[i]) == 0) return true;
    return false;
}

bool isURLValid(const char *value) {
    return strstr(value, "://") != NULL && strlen(value) > 8;
}

bool isRatio(const char *value) {
    char *end;
    float ratio = strtof(value, &end);
    return *end == '\0' && ratio >= 0.0f && ratio <= 1.0f;
}

bool isCompressionCodec(const char *value) {
    const char *valid[] = {"none", "gzip", "snappy", "lzo", "bzip2", "zstd", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcasecmp(value, valid[i]) == 0) return true;
    return false;
}

bool isExecutionModeValid(const char *value) {
    return strcasecmp(value, "mapreduce") == 0 || strcasecmp(value, "tez") == 0;
}

bool isTimezoneValid(const char *value) {
    // Simple check for format (more comprehensive validation would require TZ database lookup)
    return strchr(value, '/') != NULL && strlen(value) > 3;
}


// Atlas-specific helper functions
bool isHostPortList(const char *value) {
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    bool valid = true;
    
    while (token != NULL) {
        char *colon = strchr(token, ':');
        if (!colon || !isValidPort(colon + 1)) {
            valid = false;
            break;
        }
        token = strtok(NULL, ",");
    }
    
    free(copy);
    return valid;
}


bool fileExists(const char *path) {
    return access(path, R_OK) == 0;
}

