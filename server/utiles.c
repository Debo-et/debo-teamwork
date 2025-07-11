#include "utiles.h"
#include "connect.h"
#include "protocol.h"
#include <libssh/libssh.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>

#include <libxml/parser.h>
#include <libxml/tree.h>
 
 
extern bool dependency;

static char *
simple_prompt_extended(const char *prompt, bool echo,
					   PromptInterruptContext *prompt_ctx);
char *
apache_strdup(const char *in);
void printBorder(const char *start, const char *end, const char *color);
void printTextBlock(const char *text, const char *textColor, const char *borderColor);



/*
 * Execute a shell command and capture its output.
 *
 * Returns:
 * - A dynamically allocated string containing the command output on success.
 * - NULL on failure (memory allocation failure or command execution error).
 *
 * The caller is responsible for freeing the returned string.
 */
static char *
execute_command(const char *command)
{
    FILE   *fp;
    char    buffer[1024];
    char   *result = NULL;
    size_t  total_size = 0;
    size_t  bytes_read;

    /* Validate input */
    if (command == NULL)
        return NULL;

    /* Open a process to execute the command */
    fp = popen(command, "r");
    if (fp == NULL)
        return NULL;

    /* Read output in chunks */
    while ((bytes_read = fread(buffer, 1, sizeof(buffer), fp)) > 0)
    {
        char *temp = realloc(result, total_size + bytes_read + 1); /* +1 for null terminator */
        if (temp == NULL)
        {
            free(result);
            pclose(fp);
            return NULL;
        }
        result = temp;
        memcpy(result + total_size, buffer, bytes_read);
        total_size += bytes_read;
        result[total_size] = '\0'; /* Null-terminate after each chunk */
    }

    /* Handle read errors */
    if (ferror(fp))
    {
        free(result);
        pclose(fp);
        return NULL;
    }

    /* Close the process */
    pclose(fp);

    return result;
}



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

void
execute_and_print(const char *command)
{
    char *output;
    
  //  if (port || host || user || password)
    	// Check all connection parameters are present
    //	output = execute_remote_script(command , user, host, password);
    //else  
    	output = execute_command(command);

    if (output != NULL)
    {
        printBorder("├", "┤", YELLOW);
        printTextBlock(output, CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
    }
    else
    {
        fprintf(stderr, "Error executing command: %s\n", command);
    }
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

static int saved_stdout = -1;
static int pipefd[2] = {-1, -1};
static pthread_t capture_thread;
static bool capturing = false;
static char *config_abs_path = NULL;

static void* output_thread() {
    // Get current working directory for absolute path
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd))) {
        config_abs_path = malloc(strlen(cwd) + strlen("/configuration.txt") + 1);
        sprintf(config_abs_path, "%s/configuration.txt", cwd);
    } else {
        perror("getcwd() failed");
        config_abs_path = strdup("configuration.txt");
    }

    FILE *log_file = fopen(config_abs_path, "w");
    if (!log_file) {
        char error[512];
        snprintf(error, sizeof(error), "Error: Failed to create %s\n", config_abs_path);
        write(saved_stdout, error, strlen(error));
    }

    char buffer[4096];
    while (1) {
        ssize_t count = read(pipefd[0], buffer, sizeof(buffer));
        if (count <= 0) {
            if (count == -1 && errno == EINTR) continue;
            break;
        }
        
        // Write to original terminal
        write(saved_stdout, buffer, count);
        
        // Write to log file
        if (log_file) {
            fwrite(buffer, 1, count, log_file);
            fflush(log_file);
        }
    }

    if (log_file) fclose(log_file);
    close(pipefd[0]);
    return NULL;
}

int start_stdout_capture(void) {
    if (capturing) {
        fprintf(stderr, "Error: stdout capture already active\n");
        return -1;
    }

    fflush(stdout);

    if (pipe(pipefd) == -1) {
        perror("pipe() failed");
        return -1;
    }

    saved_stdout = dup(STDOUT_FILENO);
    if (saved_stdout == -1) {
        perror("dup() failed");
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    if (dup2(pipefd[1], STDOUT_FILENO) == -1) {
        perror("dup2() failed");
        close(pipefd[0]);
        close(pipefd[1]);
        close(saved_stdout);
        return -1;
    }
    close(pipefd[1]);

    if (pthread_create(&capture_thread, NULL, output_thread, NULL)) {
        perror("pthread_create() failed");
        dup2(saved_stdout, STDOUT_FILENO);
        close(saved_stdout);
        close(pipefd[0]);
        return -1;
    }

    capturing = true;
    return 0;
}

int stop_stdout_capture(void) {
    if (!capturing) return 0;

    fflush(stdout);
    dup2(saved_stdout, STDOUT_FILENO);
    close(saved_stdout);
    saved_stdout = -1;

    close(pipefd[0]);
    pthread_join(capture_thread, NULL);
    
    // Print absolute path if available
    if (config_abs_path) {
        // Verify file was created
        if (access(config_abs_path, F_OK) == 0) {
            printf("Configuration data saved to: %s\n", config_abs_path);
        } else {
            fprintf(stderr, "Warning: Expected output file not found: %s\n", config_abs_path);
        }
        free(config_abs_path);
        config_abs_path = NULL;
    } else {
        fprintf(stderr, "Error: Could not determine output file path\n");
    }

    capturing = false;
    return 0;
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
    const int maxLineLength = BOX_WIDTH - 4;  // Maximum text per line
    char lineBuffer[BOX_WIDTH - 2];          // Buffer for each line (+ null terminator)

    while (*start) {
        const char *end = start;
        const char *lastSpace = NULL;
        int remaining = maxLineLength;

        // Find next break point (newline, space, or max length)
        while (*end && *end != '\n' && remaining > 0) {
            if (*end == ' ') {
                lastSpace = end;  // Track last space for word wrapping
            }
            end++;
            remaining--;
        }

        // Handle breaks: newline takes priority, then space, then forced break
        if (*end == '\n') {
            // Break at newline
        } else if (lastSpace && remaining == 0) {
            end = lastSpace;  // Break at last space to avoid splitting words
        } else if (*end == '\0') {
            // Reached end of string
        }

        // Calculate segment length and copy to buffer
        int length = end - start;
        strncpy(lineBuffer, start, length);
        lineBuffer[length] = '\0';

        // Print formatted line within borders
        printf("%s│%s %s%-*s%s %s│%s\n", 
               borderColor, RESET, 
               textColor, maxLineLength, lineBuffer, RESET,
               borderColor, RESET);

        // Update start position (skip break characters)
        if (*end == '\n') {
            start = end + 1;  // Skip newline
        } else if (lastSpace && end == lastSpace) {
            start = end + 1;  // Skip space
        } else {
            start = end;      // No skip (mid-word break)
        }
    }
}

char *concatenate_strings(const char *s1, const char *s2) {
    // Handle case where both parameters are NULL
    if (s1 == NULL && s2 == NULL) {
        return NULL;
    }
    
    // Handle case where one parameter is NULL
    if (s1 == NULL) {
        return strdup(s2);
    }
    if (s2 == NULL) {
        return strdup(s1);
    }
    
    // Calculate lengths and allocate memory
    size_t len1 = strlen(s1);
    size_t len2 = strlen(s2);
    char *result = malloc(len1 + len2 + 2); // +1 for comma, +1 for null terminator
    
    if (result == NULL) {
        return NULL; // Memory allocation failure
    }
    
    // Construct the concatenated string
    strcpy(result, s1);
    strcat(result, ",");
    strcat(result, s2);
    
    return result;
}

int validate_file_path(const char* path) {
    // Check for NULL pointer
    if (path == NULL) {
        return 0;
    }

    // Get string length and perform basic length checks
    size_t len = strlen(path);
    if (len == 0 || len >= PATH_MAX) {  // PATH_MAX includes null terminator
        return 0;
    }

    size_t component_length = 0;

    // Iterate through each character in the path
    for (size_t i = 0; i < len; ++i) {
        if (path[i] == '/') {
            // Check component length when encountering a separator
            if (component_length > NAME_MAX) {
                return 0;
            }
            component_length = 0;
        } else {
            // Increment component length and check
            if (++component_length > NAME_MAX) {
                return 0;
            }
        }
    }

    // Check the final component after last separator
    if (component_length > NAME_MAX) {
        return 0;
    }

    // All checks passed
    return 1;
}


unsigned char get_protocol_code(Component comp, Action action, const char* version) {
    switch (comp) {
        case HDFS:
            switch (action) {
                case START:      return CliMsg_Hdfs_Start;
                case STOP:       return CliMsg_Hdfs_Stop;
                case RESTART:    return CliMsg_Hdfs_Restart;
                case UNINSTALL:  return CliMsg_Hdfs_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Hdfs_Install_Version;
                    else return CliMsg_Hdfs_Install;
                case CONFIGURE:  return CliMsg_Hdfs_Configure;
                case NONE:  return CliMsg_Hdfs;
                default: return 0;
            }
        case HBASE:
            switch (action) {
                case START:      return CliMsg_HBase_Start;
                case STOP:       return CliMsg_HBase_Stop;
                case RESTART:    return CliMsg_HBase_Restart;
                case UNINSTALL:  return CliMsg_HBase_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_HBase_Install_Version;
                    else return CliMsg_HBase_Install;
                case CONFIGURE:  return CliMsg_HBase_Configure;
                case NONE:  return CliMsg_HBase;
                default: return 0;
            }
        case SPARK:
            switch (action) {
                case START:      return CliMsg_Spark_Start;
                case STOP:       return CliMsg_Spark_Stop;
                case RESTART:    return CliMsg_Spark_Restart;
                case UNINSTALL:  return CliMsg_Spark_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Spark_Install_Version;
                    else return CliMsg_Spark_Install;
                case CONFIGURE:  return CliMsg_Spark_Configure;
                case NONE:  return CliMsg_Spark;
                default: return 0;
            }
        case KAFKA:
            switch (action) {
                case START:      return CliMsg_Kafka_Start;
                case STOP:       return CliMsg_Kafka_Stop;
                case RESTART:    return CliMsg_Kafka_Restart;
                case UNINSTALL:  return CliMsg_Kafka_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Kafka_Install_Version;
                    else return CliMsg_Kafka_Install;
                case CONFIGURE:  return CliMsg_Kafka_Configure;
                case NONE:  return CliMsg_Kafka;
                default: return 0;
            }
        case ZOOKEEPER:
            switch (action) {
                case START:      return CliMsg_ZooKeeper_Start;
                case STOP:       return CliMsg_ZooKeeper_Stop;
                case RESTART:    return CliMsg_ZooKeeper_Restart;
                case UNINSTALL:  return CliMsg_ZooKeeper_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_ZooKeeper_Install_Version;
                    else return CliMsg_ZooKeeper_Install;
                case CONFIGURE:  return CliMsg_ZooKeeper_Configure;
                case NONE:  return CliMsg_ZooKeeper;
                default: return 0;
            }
        case FLINK:
            switch (action) {
                case START:      return CliMsg_Flink_Start;
                case STOP:       return CliMsg_Flink_Stop;
                case RESTART:    return CliMsg_Flink_Restart;
                case UNINSTALL:  return CliMsg_Flink_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Flink_Install_Version;
                    else return CliMsg_Flink_Install;
                case CONFIGURE:  return CliMsg_Flink_Configure;
                case NONE:  return CliMsg_Flink;
                default: return 0;
            }
        case STORM:
            switch (action) {
                case START:      return CliMsg_Storm_Start;
                case STOP:       return CliMsg_Storm_Stop;
                case RESTART:    return CliMsg_Storm_Restart;
                case UNINSTALL:  return CliMsg_Storm_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Storm_Install_Version;
                    else return CliMsg_Storm_Install;
                case CONFIGURE:  return CliMsg_Storm_Configure;
                case NONE:  return CliMsg_Storm;
                default: return 0;
            }
        case HIVE:
            switch (action) {
                case START:      return CliMsg_Hive_Start;
                case STOP:       return CliMsg_Hive_Stop;
                case RESTART:    return CliMsg_Hive_Restart;
                case UNINSTALL:  return CliMsg_Hive_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Hive_Install_Version;
                    else return CliMsg_Hive_Install;
                case CONFIGURE:  return CliMsg_Hive_Configure;
                case NONE:  return CliMsg_Hive;
                default: return 0;
            }
        case PIG:
            switch (action) {
                case START:      return CliMsg_Pig_Start;
                case STOP:       return CliMsg_Pig_Stop;
                case RESTART:    return CliMsg_Pig_Restart;
                case UNINSTALL:  return CliMsg_Pig_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Pig_Install_Version;
                    else return CliMsg_Pig_Install;
                case CONFIGURE:  return CliMsg_Pig_Configure;
                case NONE:  return CliMsg_Pig;
                default: return 0;
            }
        case PRESTO:
            switch (action) {
                case START:      return CliMsg_Presto_Start;
                case STOP:       return CliMsg_Presto_Stop;
                case RESTART:    return CliMsg_Presto_Restart;
                case UNINSTALL:  return CliMsg_Presto_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Presto_Install_Version;
                    else return CliMsg_Presto_Install;
                case CONFIGURE:  return CliMsg_Presto_Configure;
                case NONE:  return CliMsg_Presto;
                default: return 0;
            }
        case TEZ:
            switch (action) {
                case START:      return CliMsg_Tez_Start;
                case STOP:       return CliMsg_Tez_Stop;
                case RESTART:    return CliMsg_Tez_Restart;
                case UNINSTALL:  return CliMsg_Tez_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Tez_Install_Version;
                    else return CliMsg_Tez_Install;
                case CONFIGURE:  return CliMsg_Tez_Configure;
                case NONE:  return CliMsg_Tez;
                default: return 0;
            }
        case ATLAS:
            switch (action) {
                case START:      return CliMsg_Atlas_Start;
                case STOP:       return CliMsg_Atlas_Stop;
                case RESTART:    return CliMsg_Atlas_Restart;
                case UNINSTALL:  return CliMsg_Atlas_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Atlas_Install_Version;
                    else return CliMsg_Atlas_Install;
                case CONFIGURE:  return CliMsg_Atlas_Configure;
                case NONE:  return CliMsg_Atlas;
                default: return 0;
            }
        case RANGER:
            switch (action) {
                case START:      return CliMsg_Ranger_Start;
                case STOP:       return CliMsg_Ranger_Stop;
                case RESTART:    return CliMsg_Ranger_Restart;
                case UNINSTALL:  return CliMsg_Ranger_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Ranger_Install_Version;
                    else return CliMsg_Ranger_Install;
                case CONFIGURE:  return CliMsg_Ranger_Configure;
                case NONE:  return CliMsg_Ranger;
                default: return 0;
            }
        case LIVY:
            switch (action) {
                case START:      return CliMsg_Livy_Start;
                case STOP:       return CliMsg_Livy_Stop;
                case RESTART:    return CliMsg_Livy_Restart;
                case UNINSTALL:  return CliMsg_Livy_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Livy_Install_Version;
                    else return CliMsg_Livy_Install;
                case CONFIGURE:  return CliMsg_Livy_Configure;
                case NONE:  return CliMsg_Livy;
                default: return 0;
            }
        case PHOENIX:
            switch (action) {
                case START:      return CliMsg_Phoenix_Start;
                case STOP:       return CliMsg_Phoenix_Stop;
                case RESTART:    return CliMsg_Phoenix_Restart;
                case UNINSTALL:  return CliMsg_Phoenix_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Phoenix_Install_Version;
                    else return CliMsg_Phoenix_Install;
              case CONFIGURE:  return CliMsg_Phoenix_Configure;
              case NONE:  return CliMsg_Phoenix;
                default: return 0;
            }
        case SOLR:
            switch (action) {
                case START:      return CliMsg_Solr_Start;
                case STOP:       return CliMsg_Solr_Stop;
                case RESTART:    return CliMsg_Solr_Restart;
                case UNINSTALL:  return CliMsg_Solr_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Solr_Install_Version;
                    else return CliMsg_Solr_Install;
                case CONFIGURE:  return CliMsg_Solr_Configure;
                case NONE:  return CliMsg_Solr;
                default: return 0;
            }
        case ZEPPELIN:
            switch (action) {
                case START:      return CliMsg_Zeppelin_Start;
                case STOP:       return CliMsg_Zeppelin_Stop;
                case RESTART:    return CliMsg_Zeppelin_Restart;
                case UNINSTALL:  return CliMsg_Zeppelin_Uninstall;
                case INSTALL:
                    if (version && *version) return CliMsg_Zeppelin_Install_Version;
                    else return CliMsg_Zeppelin_Install;
                case CONFIGURE:  return CliMsg_Zeppelin_Configure;
                case NONE:  return CliMsg_Zeppelin;
                default: return 0;
            }
        default:
            return 0;
    }
}

const char* component_to_string(Component comp) {
    switch(comp) {
        // Flink
        case FLINK: return "Flink";

        // Hadoop
        case HDFS: return "hdfs";
        // HBase
        case HBASE: return "Hbase";

        // Hive
        case HIVE: return "Hive";
        case HIVE_METASTORE: return "hive-metastore";
        // Kafka
        case KAFKA: return "Kafka";

        // Livy
        case LIVY: return "Livy";

        // Phoenix
        case PHOENIX: return "Phoenix";

        // Ranger
        case RANGER: return "Ranger";

        // Solr
        case SOLR: return "Solr";
        // Tez
        case TEZ: return "Tez";
        case ATLAS: return "Atlas";
        case STORM: return "Storm";
        case PIG: return "Pig";
        case SPARK: return "Spark";
        case PRESTO: return "Presto";

        // Zeppelin
        case ZEPPELIN: return "Zeppelin";

        // Zookeeper
        case ZOOKEEPER: return "Zookeeper";
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
        case VERSION_SWITCH:
            return "switching...";
        case UNINSTALL: 
            return "Uninstalling...";
        case CONFIGURE:
            return "Configuring...";
        case NONE:
            return "Reporting...";
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



Conn* connect_to_debo(const char* host, const char* port) {
    const char* keywords[5] = {NULL};  // Connection parameters + NULL terminator
    const char* values[5] = {NULL};
    int param_index = 0;

    // Populate connection parameters (skip NULL or empty values)
    if (host && host[0]) {
        keywords[param_index] = "host";
        values[param_index] = host;
        param_index++;
    }
    if (port && port[0]) {
        keywords[param_index] = "port";
        values[param_index] = port;
        param_index++;
    }


    // Terminate the parameter arrays
    keywords[param_index] = NULL;
    values[param_index] = NULL;

    // Establish database connection using parameter arrays
    Conn* connection = connectStartParams(keywords, values);

    // Verify connection success
    if (connection->status != CONNECTION_STARTED) {
        fprintf(stderr, "Debo connection error: \n");
        return NULL;
    }

    return connection;
}
void reset_connection_buffers(Conn *conn) {
    if (conn == NULL) {
        return;
    }

    /* Free existing buffers */
    free(conn->inBuffer);
    free(conn->outBuffer);

    /* Reinitialize input buffer */
    conn->inBufSize = 16 * 1024;
    conn->inBuffer = (char *)malloc(conn->inBufSize);
    conn->inStart = 0;
    conn->inCursor = 0;
    conn->inEnd = 0;

    /* Reinitialize output buffer */
    conn->outBufSize = 16 * 1024;
    conn->outBuffer = (char *)malloc(conn->outBufSize);
    conn->outCount = 0;
    conn->outMsgStart = -1;  /* Indicates no incomplete message */
    conn->outMsgEnd = 0;
}

Component* get_dependencies(Component comp, int *count) {
    static Component hbase_deps[] = {HDFS, ZOOKEEPER};
    static Component kafka_deps[] = {ZOOKEEPER};
    static Component hive_deps[] = {HDFS, TEZ}; // Added YARN/TEZ
    static Component phoenix_deps[] = {HBASE};
    static Component storm_deps[] = {ZOOKEEPER};
    static Component spark_deps[] = {HDFS};    // Added YARN
    static Component tez_deps[] = {HDFS};      // Added YARN
    static Component livy_deps[] = {SPARK};
    static Component ranger_deps[] = {SOLR, HDFS};   // Added HDFS
    static Component atlas_deps[] = {HBASE, KAFKA, SOLR}; 
    static Component pig_deps[] = {HDFS};
    static Component solr_deps[] = {ZOOKEEPER};
    static Component yarn_deps[] = {HDFS};
    static Component flink_deps[] = {HDFS, YARN};

    switch (comp) {
        case HBASE: *count = 2; return hbase_deps;
        case KAFKA: *count = 1; return kafka_deps;
        case HIVE: *count = 3; return hive_deps;          // Updated count
        case PHOENIX: *count = 1; return phoenix_deps;
        case STORM: *count = 1; return storm_deps;
        case SPARK: *count = 2; return spark_deps;        // Updated count
        case TEZ: *count = 2; return tez_deps;            // Updated count
        case LIVY: *count = 1; return livy_deps;
        case RANGER: *count = 2; return ranger_deps;      // Updated count
        // New cases for previously missing components
        case ATLAS: *count = 3; return atlas_deps;
        case PIG: *count = 2; return pig_deps;
        case SOLR: *count = 1; return solr_deps;
        case YARN: *count = 1; return yarn_deps;
        case FLINK: *count = 2; return flink_deps;
        // Components with no dependencies
        case PRESTO: 
        case ZEPPELIN: 
        case ZOOKEEPER: 
        case HDFS: 
        default: *count = 0; return NULL;
    }
}


/**
 * Sends a command message based on the specified protocol.
 * 
 * @param component The target component for the action.
 * @param action The action to be performed.
 * @param version Version string for installation (nullable).
 * @param location Location string for installation (nullable).
 * @param param_name Parameter name for configuration (nullable).
 * @param param_value Parameter value for configuration (nullable).
 * @param conn The connection object for network operations.
 * @return 1 on success, 0 on failure.
 */
void SendComponentActionCommand(Component component, Action action,
                              const char* version,
                              const char* param_name, const char* param_value,
                              Conn* conn) {
    if (!conn) {
        fprintf(stderr, "Invalid connection object\n");
        return;
    }
    int dep_count;

    if (action == VERSION_SWITCH) // Only for INSTALL action
    {
            SendComponentActionCommand(component, UNINSTALL, NULL, NULL, NULL, conn);
            SendComponentActionCommand(component, INSTALL, version, NULL, NULL, conn);
         //   configure_dependency_for_remote_component(component, deps[i], conn);
    }

    if (dependency && (action == INSTALL || action == UNINSTALL)) // Only for INSTALL action
    {
        Component *deps = get_dependencies(component, &dep_count);
      //  printTextBlock("Installing dependency ...\n", CYAN, YELLOW);
        for (int i = 0; i < dep_count; i++) {
          //  char buffer[256];
           // const char* compStr = component_to_string(deps[i]);
         //   snprintf(buffer, sizeof(buffer),
           //     "Installing dependency ...\n");
            //printTextBlock("Installing dependency ...\n", CYAN, YELLOW);
            
            // Send dependency command
            SendComponentActionCommand(deps[i], action, NULL, NULL, NULL, conn);
         //   configure_dependency_for_remote_component(component, deps[i], conn);

        }
    }
    // Convert enums to protocol codes
    unsigned char comp_code = get_protocol_code(component, action, version);

    // Send component and action codes
    if (PutMsgStart(comp_code, conn) < 0) {
        fprintf(stderr, "Failed to send component/action\n");
        return;
    }
    // Handle action-specific parameters
    switch (action) {
        case START:
        PutMsgEnd(conn);
        break;
        case STOP:
        PutMsgEnd(conn);
        break;
        case RESTART:
        PutMsgEnd(conn);
        break;
        case UNINSTALL:
        PutMsgEnd(conn);
        break;

        case CONFIGURE:
            if (!param_name || !param_value) {
                fprintf(stderr, "Invalid configuration parameters\n");
                return;
            }
            char *result = concatenate_strings(param_name, param_value);

            if (Putnchar(result, strlen(result), conn) < 0) {
                fprintf(stderr, "Failed to send configuration parameters\n");
                return;
            }
            PutMsgEnd(conn);
            break;

        case INSTALL:
            // Send version if provided
            
            if (version) {
                if (Putnchar(version, strlen(version), conn) < 0) {
                    fprintf(stderr, "Failed to send version\n");
                    return;
                }
            }
            PutMsgEnd(conn);
            break;

        case NONE:
        PutMsgEnd(conn);
            // No additional parameters needed
            break;
        default:
            return;
            return;
    }
 (void) Flush(conn);

}

bool executeSystemCommand(const char *cmd) {
    int ret = system(cmd);
    
    if (ret == -1) {
        // Only report system() execution failures
        perror("system() execution failed");
        return false;
    }

    // Handle command execution results without reporting non-zero exits
    if (WIFSIGNALED(ret)) {
        fprintf(stderr, "Command terminated abnormally (signal %d)\n", WTERMSIG(ret));
    }
    
    return true;
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

static bool is_directory(const char* path) {
    if (path == NULL) return false;
    struct stat st;
    if (stat(path, &st) != 0) return false;
    return S_ISDIR(st.st_mode);
}


static const char* get_default_base_path() {
    FILE* f = fopen("/etc/os-release", "r");
    if (f == NULL) return NULL;

    char line[256];
    const char* base_path = NULL;
    while (fgets(line, sizeof(line), f) != NULL) {
        if (strncmp(line, "ID=", 3) == 0) {
            char* id_value = line + 3;
            char* end = strchr(id_value, '\n');
            if (end) *end = '\0';

            if (*id_value == '"') {
                memmove(id_value, id_value + 1, strlen(id_value));
                end = strchr(id_value, '"');
                if (end) *end = '\0';
            }

            if (strcmp(id_value, "ubuntu") == 0 || strcmp(id_value, "debian") == 0) {
                base_path = "/usr/local";
                break;
            } else if (strcmp(id_value, "centos") == 0 || strcmp(id_value, "rhel") == 0 ||
                       strcmp(id_value, "fedora") == 0 || strcmp(id_value, "rocky") == 0) {
                base_path = "/opt";
                break;
            }
        }
    }
    fclose(f);
    return base_path;
}

static void get_component_info(Component comp, const char **env_var, const char **fallback_dir, const char **conf_subdir) {
    *env_var = NULL;
    *fallback_dir = NULL;
    *conf_subdir = NULL;

    switch (comp) {
        case HDFS:
        case YARN:
            *env_var = "HADOOP_HOME";
            *fallback_dir = "hadoop";
            *conf_subdir = "etc/hadoop";
            break;
        case HBASE:
            *env_var = "HBASE_HOME";
            *fallback_dir = "hbase";
            *conf_subdir = "conf";
            break;
        case HIVE:
        case HIVE_METASTORE:
            *env_var = "HIVE_HOME";
            *fallback_dir = "hive";
            *conf_subdir = "conf";
            break;
        case KAFKA:
            *env_var = "KAFKA_HOME";
            *fallback_dir = "kafka";
            *conf_subdir = "config";
            break;
        case LIVY:
            *env_var = "LIVY_HOME";
            *fallback_dir = "livy";
            *conf_subdir = "conf";
            break;
        case PHOENIX:
            *env_var = "PHOENIX_HOME";
            *fallback_dir = "phoenix";
            *conf_subdir = "conf";
            break;
        case STORM:
            *env_var = "STORM_HOME";
            *fallback_dir = "storm";
            *conf_subdir = "conf";
            break;
        case HUE:
            *env_var = "HUE_HOME";
            *fallback_dir = "hue";
            *conf_subdir = "desktop/conf";
            break;
        case PIG:
            *env_var = "PIG_HOME";
            *fallback_dir = "pig";
            *conf_subdir = "conf";
            break;
        case OOZIE:
            *env_var = "OOZIE_HOME";
            *fallback_dir = "oozie";
            *conf_subdir = "conf";
            break;
        case PRESTO:
            *env_var = "PRESTO_HOME";
            *fallback_dir = "presto";
            *conf_subdir = "etc";
            break;
        case ATLAS:
            *env_var = "ATLAS_HOME";
            *fallback_dir = "atlas";
            *conf_subdir = "conf";
            break;
        case RANGER:
            *env_var = "RANGER_HOME";
            *fallback_dir = "ranger";
            *conf_subdir = "conf";
            break;
        case SOLR:
            *env_var = "SOLR_HOME";
            *fallback_dir = "solr";
            *conf_subdir = "etc";
            break;
        case SPARK:
            *env_var = "SPARK_HOME";
            *fallback_dir = "spark";
            *conf_subdir = "conf";
            break;
        case TEZ:
            *env_var = "TEZ_HOME";
            *fallback_dir = "tez";
            *conf_subdir = "conf";
            break;
        case ZEPPELIN:
            *env_var = "ZEPPELIN_HOME";
            *fallback_dir = "zeppelin";
            *conf_subdir = "conf";
            break;
        case ZOOKEEPER:
            *env_var = "ZOOKEEPER_HOME";
            *fallback_dir = "zookeeper";
            *conf_subdir = "conf";
            break;
        case FLINK:
            *env_var = "FLINK_HOME";
            *fallback_dir = "flink";
            *conf_subdir = "conf";
            break;
        default:
            break;
    }
}

char* get_component_config_path(Component comp, const char* config_filename) {
    if (comp == NONE || config_filename == NULL) return NULL;

    const char *env_var = NULL;
    const char *fallback_dir = NULL;
    const char *conf_subdir = NULL;
    get_component_info(comp, &env_var, &fallback_dir, &conf_subdir);

    if (env_var == NULL || fallback_dir == NULL || conf_subdir == NULL) return NULL;

    const char* env_value = getenv(env_var);
    char* base_path = NULL;

    if (env_value != NULL && is_directory(env_value)) {
        base_path = strdup(env_value);
    }

    if (base_path == NULL) {
        const char* default_base = get_default_base_path();
        if (default_base == NULL) default_base = "/opt";

        size_t len = strlen(default_base) + strlen(fallback_dir) + 2;
        base_path = malloc(len);
        if (base_path == NULL) return NULL;
        snprintf(base_path, len, "%s/%s", default_base, fallback_dir);
    }

    size_t conf_dir_len = strlen(base_path) + strlen(conf_subdir) + 2;
    char* conf_dir = malloc(conf_dir_len);
    if (conf_dir == NULL) {
        free(base_path);
        return NULL;
    }
    snprintf(conf_dir, conf_dir_len, "%s/%s", base_path, conf_subdir);
    free(base_path);

    size_t full_len = strlen(conf_dir) + strlen(config_filename) + 2;
    char* full_path = malloc(full_len);
    if (full_path == NULL) {
        free(conf_dir);
        return NULL;
    }
    snprintf(full_path, full_len, "%s/%s", conf_dir, config_filename);
    free(conf_dir);

    return full_path;
}


void handle_result(ConfigStatus status, const char *config_param, const char *config_value, const char *config_file) {
    switch(status) {
        case SUCCESS:
            printf("Successfully configure '%s' to '%s' in %s\n", 
                   config_param, config_value, config_file);
            break;
            
        case INVALID_FILE_TYPE:
            fprintf(stderr, "Error: File '%s' has invalid type (must be .xml)\n", config_file);
            break;
            
        case FILE_NOT_FOUND:
            fprintf(stderr, "Error: Configuration file '%s' not found\n", config_file);
            break;
            
        case XML_PARSE_ERROR:
            fprintf(stderr, "Error: Failed to parse XML in '%s'\n", config_file);
            break;
            
        case INVALID_CONFIG_FILE:
            fprintf(stderr, "Error: Invalid XML structure in '%s'\n", config_file);
            break;
            
        case XML_UPDATE_ERROR:
            fprintf(stderr, "Error: Failed to update parameter '%s' in '%s'\n", 
                   config_param, config_file);
            break;
            
        case FILE_WRITE_ERROR:
            fprintf(stderr, "Error: Could not write changes to '%s'\n", config_file);
            break;
            
        case FILE_READ_ERROR:
            fprintf(stderr, "Error: Could not read from '%s'\n", config_file);
            break;
            
        case XML_INVALID_ROOT:
            fprintf(stderr, "Error: Invalid/missing root element in '%s'\n", config_file);
            break;
            
        case SAVE_FAILED:
            fprintf(stderr, "Error: Failed to save configuration changes to '%s'\n", config_file);
            break;
            
        default:
            fprintf(stderr, "Unknown error occurred (status code: %d)\n", status);
            break;
    }
}

bool handleValidationResult(ValidationResult result) {
    switch (result) {
        case VALIDATION_OK:
            return true;
            
        case ERROR_PARAM_NOT_FOUND:
            fprintf(stderr, "Error: Parameter not found in  configuration\n");
            break;
            
        case ERROR_VALUE_EMPTY:
            fprintf(stderr, "Error: Configuration value cannot be empty\n");
            break;
            
        case ERROR_INVALID_FORMAT:
            fprintf(stderr, "Error: Invalid format for configuration parameter\n");
            break;
            
        case ERROR_CONSTRAINT_VIOLATED:
            fprintf(stderr, "Error: Parameter value violates constraints\n");
            break;
            
        default:
            fprintf(stderr, "Error: Unknown validation error\n");
            break;
    }
    return false;
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



#define BUFFER_SIZE 1024

int update_config(const char *param, const char *value, const char *file_path) {
    // Open file for reading
    FILE *file = fopen(file_path, "r");
    if (file == NULL) {
        // File doesn't exist - create and write new key-value pair
        FILE *new_file = fopen(file_path, "w");
        if (new_file == NULL) {
            fprintf(stderr, "Error creating file: %s\n", strerror(errno));
            return -1;
        }
        fprintf(new_file, "%s=%s\n", param, value);
        fclose(new_file);
        return 0;
    }

    // Read file into memory
    char **lines = NULL;
    size_t line_count = 0;
    char buffer[BUFFER_SIZE];
    int found = 0;

    while (fgets(buffer, sizeof(buffer), file) != NULL) {
        size_t len = strlen(buffer);
        if (len > 0 && buffer[len-1] == '\n') {
            buffer[len-1] = '\0'; // Strip newline for processing
        }

        // Allocate space for line pointer
        char **temp = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!temp) {
            perror("Memory allocation failed");
            fclose(file);
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return -1;
        }
        lines = temp;
        lines[line_count] = strdup(buffer);
        if (!lines[line_count]) {
            perror("Memory allocation failed");
            fclose(file);
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return -1;
        }
        line_count++;
    }
    fclose(file);

    // Search for parameter and update if found
    for (size_t i = 0; i < line_count; i++) {
        char *line = lines[i];
        char *p = line;

        // Skip leading whitespace
        while (isspace((unsigned char)*p)) p++;

        // Skip comments and empty lines
        if (*p == '#' || *p == '\0') continue;

        // Split into key and value
        char *delim = strchr(p, '=');
        if (!delim) continue;

        // Extract key and trim trailing whitespace
        *delim = '\0';
        char *key = p;
        char *key_end = delim - 1;
        while (key_end >= key && isspace((unsigned char)*key_end)) {
            *key_end = '\0';
            key_end--;
        }

        // Check if key matches
        if (strcmp(key, param) == 0) {
            found = 1;
            // Create new line: key=value
            char new_line[BUFFER_SIZE];
            snprintf(new_line, sizeof(new_line), "%s=%s", param, value);
            free(lines[i]);
            lines[i] = strdup(new_line);
            if (!lines[i]) {
                perror("Memory allocation failed");
                for (size_t j = 0; j < line_count; j++) free(lines[j]);
                free(lines);
                return -1;
            }
            break;
        }
    }

    // Append if not found
    if (!found) {
        // Add new line
        char **temp = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!temp) {
            perror("Memory allocation failed");
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return -1;
        }
        lines = temp;
        char new_line[BUFFER_SIZE];
        snprintf(new_line, sizeof(new_line), "%s=%s", param, value);
        lines[line_count] = strdup(new_line);
        if (!lines[line_count]) {
            perror("Memory allocation failed");
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return -1;
        }
        line_count++;
    }

    // Write updated content back to file
    FILE *out = fopen(file_path, "w");
    if (!out) {
        fprintf(stderr, "Error opening file for writing: %s\n", strerror(errno));
        for (size_t i = 0; i < line_count; i++) free(lines[i]);
        free(lines);
        return -1;
    }

    for (size_t i = 0; i < line_count; i++) {
        fprintf(out, "%s\n", lines[i]); // Write with newline
        free(lines[i]);
    }
    free(lines);
    fclose(out);
    return 0;
}



#define MAX_PATH_LENGTH 4096

int create_xml_file(const char *directory_path, const char *xml_file_name) {
    // Validate input parameters
    if (!directory_path || !xml_file_name) {
        fprintf(stderr, "Error: Null input parameters\n");
        return -1;
    }

    // Check directory path validity
    size_t dir_len = strlen(directory_path);
    if (dir_len == 0 || dir_len > MAX_PATH_LENGTH - 1) {
        fprintf(stderr, "Error: Invalid directory path length\n");
        return -1;
    }

    // Check filename validity
    size_t file_len = strlen(xml_file_name);
    if (file_len == 0 || file_len > 255 || 
        strstr(xml_file_name, "..") != NULL || 
        strchr(xml_file_name, '/') != NULL || 
        strchr(xml_file_name, '\\') != NULL) {
        fprintf(stderr, "Error: Invalid XML file name\n");
        return -1;
    }

    // Construct full file path
    char full_path[MAX_PATH_LENGTH];
    int path_len = snprintf(full_path, sizeof(full_path), "%s/%s", directory_path, xml_file_name);
    if (path_len < 0 || path_len >= (int)sizeof(full_path)) {
        fprintf(stderr, "Error: Path construction failed\n");
        return -1;
    }

    // Create directory with proper permissions
    if (mkdir(directory_path, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "Error creating directory '%s': %s\n", 
                directory_path, strerror(errno));
        return -1;
    }

    // Open file for writing
    FILE *file = fopen(full_path, "wx");
    if (!file) {
        fprintf(stderr, "Error opening file '%s': %s\n", 
                full_path, strerror(errno));
        return -1;
    }

    // Define XML content with required structure
    const char *xml_content = 
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"
        "<configuration xmlns:xi=\"http://www.w3.org/2001/XInclude\">\n"
        "</configuration>\n";

    // Write content to file
    size_t content_length = strlen(xml_content);
    size_t written = fwrite(xml_content, 1, content_length, file);
    
    // Handle write errors
    if (written != content_length) {
        fprintf(stderr, "Error writing to file: %s\n", 
                ferror(file) ? "Write failure" : "Unknown error");
        fclose(file);
        remove(full_path);
        return -1;
    }

    // Finalize file operations
    if (fclose(file) != 0) {
        fprintf(stderr, "Error closing file: %s\n", strerror(errno));
        remove(full_path);
        return -1;
    }

    return 0;
}


int create_properties_file(const char *directory_path, const char *properties_file_name) {
    // Validate input parameters
    if (directory_path == NULL || properties_file_name == NULL) {
        fprintf(stderr,  "Error: Null input parameters\n");
        return -1;
    }

    // Check directory path length
    size_t dir_len = strlen(directory_path);
    if (dir_len == 0 || dir_len >= MAX_PATH_LENGTH) {
        fprintf(stderr, "Error: Invalid directory path length\n");
        return -1;
    }

    // Validate filename
    size_t file_len = strlen(properties_file_name);
    const char *ext_properties = ".properties";
    const char *ext_ini = ".ini";
    size_t ext_properties_len = strlen(ext_properties);
    size_t ext_ini_len = strlen(ext_ini);
    
    int valid_extension = 0;
    // Check for .properties extension
    if (file_len >= ext_properties_len && 
        strcmp(properties_file_name + file_len - ext_properties_len, ext_properties) == 0) {
        valid_extension = 1;
    }
    // Check for .ini extension
    else if (file_len >= ext_ini_len && 
             strcmp(properties_file_name + file_len - ext_ini_len, ext_ini) == 0) {
        valid_extension = 1;
    }

    // Validate filename length and extension
    if (file_len == 0 || 
        file_len > 255 || 
        !valid_extension) {
        fprintf(stderr,  "Error: File name must end with '.properties' or '.ini'\n");
        return -1;
    }

    // Check for path traversal attempts
    if (strstr(properties_file_name, "..") != NULL || 
        strchr(properties_file_name, '/') != NULL || 
        strchr(properties_file_name, '\\') != NULL) {
        fprintf(stderr,  "Error: Invalid characters in file name\n");
        return -1;
    }

    // Construct full path
    char full_path[MAX_PATH_LENGTH];
    int path_len = snprintf(full_path, sizeof(full_path), "%s/%s", directory_path, properties_file_name);
    if (path_len < 0 || path_len >= (int)sizeof(full_path)) {
        fprintf(stderr, "Error: Path construction failed. Path too long\n");
        return -1;
    }

    // Create directory if needed (with secure permissions)
    if (mkdir(directory_path, 0755) != 0) {
        if (errno != EEXIST) {
            fprintf(stderr,  "Error creating directory '%s': %s\n", 
                    directory_path, strerror(errno));
            return -1;
        }
        // Directory already exists - verify it's actually a directory
        else {
            struct stat dir_stat;
            if (stat(directory_path, &dir_stat) != 0 || !S_ISDIR(dir_stat.st_mode)) {
                fprintf(stderr,  "Error: Path exists but is not a directory\n");
                return -1;
            }
        }
    }

    // Create file with exclusive mode (fails if file exists)
    FILE *file = fopen(full_path, "wx");
    if (file == NULL) {
        fprintf(stderr,  "Error creating file '%s': %s\n", 
                full_path, strerror(errno));
        return -1;
    }

    // Close file handle (no content written per requirements)
    if (fclose(file) != 0) {
        fprintf(stderr,  "Error closing file: %s\n", strerror(errno));
        remove(full_path);  // Clean up partially created file
        return -1;
    }

    return 0;  // Success
}

#define MAX_FILENAME_LENGTH 256

int create_conf_file(const char *directory_path, const char *conf_file_name) {
    // Validate input parameters
    if (directory_path == NULL || conf_file_name == NULL) {
        fprintf(stderr, "Error: Null input parameters\n");
        return -1;
    }

    // Check directory path length
    size_t dir_len = strlen(directory_path);
    if (dir_len == 0 || dir_len >= MAX_PATH_LENGTH) {
        fprintf(stderr, "Error: Invalid directory path length (0-%d allowed)\n", MAX_PATH_LENGTH-1);
        return -1;
    }

    // Validate filename
    size_t file_len = strlen(conf_file_name);
    const char *extension = ".conf";
    size_t ext_len = strlen(extension);
    
    if (file_len == 0 || file_len >= MAX_FILENAME_LENGTH) {
        fprintf(stderr, "Error: Invalid filename length (1-%d allowed)\n", MAX_FILENAME_LENGTH-1);
        return -1;
    }
    
    // Ensure filename ends with .conf extension
    if (file_len < ext_len || strcmp(conf_file_name + file_len - ext_len, extension) != 0) {
        fprintf(stderr, "Error: Filename must end with '.conf' extension\n");
        return -1;
    }

    // Check for invalid characters in filename
    if (strstr(conf_file_name, "..") != NULL || 
        strchr(conf_file_name, '/') != NULL || 
        strchr(conf_file_name, '\\') != NULL ||
        strchr(conf_file_name, ':') != NULL ||
        strchr(conf_file_name, '*') != NULL ||
        strchr(conf_file_name, '?') != NULL ||
        strchr(conf_file_name, '"') != NULL ||
        strchr(conf_file_name, '<') != NULL ||
        strchr(conf_file_name, '>') != NULL ||
        strchr(conf_file_name, '|') != NULL) {
        fprintf(stderr, "Error: Invalid characters in filename\n");
        return -1;
    }

    // Construct full path safely
    char full_path[MAX_PATH_LENGTH];
    int path_len = snprintf(full_path, sizeof(full_path), "%s/%s", directory_path, conf_file_name);
    if (path_len < 0 || path_len >= (int)sizeof(full_path)) {
        fprintf(stderr, "Error: Path construction failed (max %d chars)\n", MAX_PATH_LENGTH-1);
        return -1;
    }

    // Create directory if needed (with secure permissions)
    if (mkdir(directory_path, 0755) != 0) {
        if (errno != EEXIST) {
            fprintf(stderr, "Error creating directory '%s': %s\n", 
                    directory_path, strerror(errno));
            return -1;
        }
        
        // Verify existing path is a directory
        struct stat path_stat;
        if (stat(directory_path, &path_stat) != 0) {
            fprintf(stderr, "Error accessing directory '%s': %s\n",
                    directory_path, strerror(errno));
            return -1;
        }
        
        if (!S_ISDIR(path_stat.st_mode)) {
            fprintf(stderr, "Error: Path exists but is not a directory\n");
            return -1;
        }
    }

    // Create file exclusively (fails if exists)
    FILE *file = fopen(full_path, "wx");
    if (file == NULL) {
        // Provide specific error for file existence
        if (errno == EEXIST) {
            fprintf(stderr, "Error: File '%s' already exists\n", full_path);
        } else {
            fprintf(stderr, "Error creating file '%s': %s\n", 
                    full_path, strerror(errno));
        }
        return -1;
    }

    // Close file handle (creating empty file)
    if (fclose(file) != 0) {
        fprintf(stderr, "Error closing file '%s': %s\n", 
                full_path, strerror(errno));
        remove(full_path);  // Clean up empty file
        return -1;
    }

    // Set restrictive permissions (owner read/write only)
    if (chmod(full_path, 0600) != 0) {
        fprintf(stderr, "Warning: Failed to set permissions on '%s': %s\n",
                full_path, strerror(errno));
        // Not fatal, but warn about potential permission issues
    }

    return 0;  // Success
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

