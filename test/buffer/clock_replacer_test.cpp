#include "buffer/clock_replacer.h"
#include "gtest/gtest.h"

// Test case for the CLOCKReplacer class.
TEST(ClockReplacerTest, SampleTest) {
  // Create a CLOCKReplacer with a capacity of 7 frames.
  CLOCKReplacer clock_replacer(7);

  // Scenario: Unpin several elements to add them to the replacer.
  clock_replacer.Unpin(1);
  clock_replacer.Unpin(2);
  clock_replacer.Unpin(3);
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(5);
  // Initially, 5 frames are in the replacer.
  EXPECT_EQ(5, clock_replacer.Size());

  // Scenario: Find a victim. The clock hand cycles through 1,2,3,4,5,
  // setting their reference bits to 0. It then wraps around and finds
  // frame 1 as the first victim (since its ref bit is now 0).
  int value;
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);

  // The next victim should be frame 2, as the clock hand continues from there.
  clock_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  // After two evictions, the size should be 3.
  EXPECT_EQ(3, clock_replacer.Size());

  // Scenario: Pin a frame that is currently in the replacer.
  clock_replacer.Pin(4);
  // The size should decrease to 2.
  EXPECT_EQ(2, clock_replacer.Size());

  // Scenario: Unpin a frame that is already in the replacer.
  // Its reference bit should be set to 1.
  clock_replacer.Unpin(3);
  EXPECT_EQ(2, clock_replacer.Size());

  // Scenario: Unpin a new frame.
  clock_replacer.Unpin(6);
  // Size should increase to 3. The new list order is conceptually {3, 5, 6}.
  EXPECT_EQ(3, clock_replacer.Size());

  // Scenario: Find more victims.
  // Hand is at frame 3. Its ref bit was just set to 1. It is set to 0, hand moves on.
  // Hand is at frame 5. Its ref bit is 0. Victim is 5.
  clock_replacer.Victim(&value);
  EXPECT_EQ(5, value);

  // Hand is at frame 6. Its ref bit is 1. It is set to 0, hand moves on.
  // Hand is at frame 3. Its ref bit is now 0. Victim is 3.
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  // The final victim should be 6.
  clock_replacer.Victim(&value);
  EXPECT_EQ(6, value);

  // The replacer should now be empty.
  EXPECT_EQ(0, clock_replacer.Size());
}