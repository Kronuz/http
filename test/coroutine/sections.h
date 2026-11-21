#pragma once

// The coroutine test's sections, each run by main() in order; each takes the shared application.

#include "app.h"

void section_A(App& app);
void section_C(App& app);
void section_J(App& app);
void section_D(App& app);
void section_S(App& app);
void section_E(App& app);
void section_F(App& app);
void section_G(App& app);
void section_H(App& app);
void section_I(App& app);
void section_T(App& app);
void section_X(App& app);
void body_stream_tests();
bool affinity_death_test(const std::string& request);
