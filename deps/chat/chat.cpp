#include <regex>
#include "chat.h"
#include "openai/openai.hpp"
#include <cstring>


int chat_init(void)
{
    try {
        openai::start("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");
    } catch(const std::runtime_error& re) {
        std::cerr << "Runtime error: " << re.what() << std::endl;
        return -1;
    } catch(const std::exception& ex) {
        std::cerr << "Error occurred: " << ex.what() << std::endl;
        return -1;
    } catch(...) {
        std::cerr << "Unknown failure occurred. Possible memory corruption" << std::endl;
        return -1;
    }

    return 0;
}

char *chat_get(const char *request)
{
    try {
        std::string result;
        std::string req("Pretend to be an expert redis lua script writer."
                        "Write redis lua script for the following scenarios. "
                        "Use ' instead of \" in lua scripts as string quote character!"
                        "Escape \" character with \\ if it exists in the script!."
                        "Never write scripts that expect an argument or key name! e.g. script must not contain KEYS[1] or KEYS[2] or ARGV[1]"
                        "Do not give any explanation!. "
                        "Do not reply with text other than lua script itself!."
        );
        req += request;

        nlohmann::json json = {
                {"model",       "gpt-3.5-turbo"},
                {"messages", {
                                        { {"role", "user"}, {"content", req.c_str()}},
                                }
                },
                {"max_tokens",  3000},
                {"temperature", 0}
        };

        auto chat = openai::chat().create(json);

        //std::cout << chat.dump() << std::endl;

        result = chat["choices"][0]["message"]["content"];
        result = std::regex_replace(result, std::regex("\\\\n"), "\n");
        result = std::string("EVAL \" \n") + result + "\n \"" + " 0";
        return strdup(result.c_str());
    } catch(const std::runtime_error& re) {
        std::cerr << "Runtime error: " << re.what() << std::endl;
        return nullptr;
    } catch(const std::exception& ex) {
        std::cerr << "Error occurred: " << ex.what() << std::endl;
        return nullptr;
    } catch(...) {
        std::cerr << "Unknown failure occurred. Possible memory corruption" << std::endl;
        return nullptr;
    }
}

char *chat_prepare(const char *response)
{
    std::string result(response);

    result = std::regex_replace(result, std::regex("\n"), " ");
    result = std::regex_replace(result, std::regex("\\'"), "\\\'");

    return strdup(result.c_str());
}
